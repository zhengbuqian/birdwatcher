package states

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

type PprofParam struct {
	framework.ParamBase `use:"pprof" desc:"get pprof from online components"`
	Type                string `name:"type" default:"goroutine" desc:"pprof metric type to fetch"`
	Port                int64  `name:"port" default:"9091" desc:"metrics port milvus component is using"`
	BeaconURL           string `name:"beacon-url" default:"" desc:"beacon base URL, overrides BW_BEACON_URL/BEACON_URL"`
	BeaconRemark        string `name:"beacon-remark" default:"" desc:"beacon remark, overrides BW_BEACON_REMARK/BEACON_REMARK"`
	BeaconMeta          string `name:"beacon-meta" default:"" desc:"beacon meta JSON, overrides BW_BEACON_META/BEACON_META"`
}

type beaconUploadConfig struct {
	URL      string
	Username string
	Remark   string
	Meta     string
}

type beaconUploadResponse struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

func (s *InstanceState) GetPprofCommand(ctx context.Context, p *PprofParam) error {
	switch p.Type {
	case "goroutine", "heap", "profile", "allocs", "block", "mutex":
	default:
		return errors.New("invalid pprof metric type provided")
	}

	sessions, err := common.ListSessions(ctx, s.client, s.basePath)
	if err != nil {
		return errors.Wrap(err, "failed to list sessions")
	}

	now := time.Now()
	filePath := fmt.Sprintf("bw_pprof_%s.%s.tar.gz", p.Type, now.Format("060102-150405"))
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}

	// Create new Writers for gzip and tar
	// These writers are chained. Writing to the tar writer will
	// write to the gzip writer which in turn will write to
	// the "buf" writer
	gw := gzip.NewWriter(f)
	tw := tar.NewWriter(gw)
	archiveClosed := false
	defer func() {
		if archiveClosed {
			return
		}
		_ = tw.Close()
		_ = gw.Close()
		_ = f.Close()
	}()

	// dedup by IP, standalone sessions share the same process/pprof endpoint
	groups := lo.GroupBy(sessions, func(session *models.Session) string {
		return session.IP()
	})

	type pprofResult struct {
		sessions []*models.Session
		data     []byte
		err      error
	}

	ch := make(chan pprofResult, len(sessions))
	signal := make(chan error, len(groups))

	go func() {
		for result := range ch {
			session := result.sessions[0]
			serverName := session.ServerName
			// set to mixture if there are multiple sessions in group
			if len(result.sessions) > 1 {
				serverName = "mixture"
			}
			if result.err != nil {
				fmt.Printf("failed to fetch %s pprof from %s-%d: %v\n", p.Type, serverName, session.ServerID, result.err)
				continue
			}
			if writeErr := tw.WriteHeader(&tar.Header{
				Typeflag: tar.TypeReg,

				Name: buildPprofArchiveEntryName(serverName, session.ServerID, p.Type),
				Size: int64(len(result.data)),
				Mode: 0o600,
			}); writeErr != nil {
				signal <- writeErr
				continue
			}

			if _, writeErr := tw.Write(result.data); writeErr != nil {
				signal <- writeErr
				continue
			}

			fmt.Printf("%s pprof from %s-%d fetched, added into archive file\n", p.Type, serverName, session.ServerID)
		}
		close(signal)
	}()

	wg := sync.WaitGroup{}
	wg.Add(len(groups))
	for _, sessions := range groups {
		go func(sessions []*models.Session) {
			defer wg.Done()
			// protection logic
			if len(sessions) == 0 {
				return
			}

			result := pprofResult{
				sessions: sessions,
			}
			addr := sessions[0].IP()
			// TODO add auto detection from configuration API
			url := fmt.Sprintf("http://%s:%d/debug/pprof/%s?debug=0", addr, p.Port, p.Type)

			// #nosec
			resp, err := http.Get(url)
			if err != nil {
				result.err = err
				ch <- result
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				errBody, _ := io.ReadAll(resp.Body)
				result.err = errors.Errorf("unexpected status code %d, body: %s", resp.StatusCode, strings.TrimSpace(string(errBody)))
				ch <- result
				return
			}

			bs, err := io.ReadAll(resp.Body)
			if err != nil {
				result.err = err
				ch <- result
				return
			}

			result.data = bs
			ch <- result
		}(sessions)
	}
	wg.Wait()
	close(ch)

	writeErr := collectPprofArchiveWriteError(signal)

	if err := closePprofArchive(tw, gw, f); err != nil {
		return err
	}
	archiveClosed = true
	if writeErr != nil {
		return errors.Wrap(writeErr, "failed to write pprof archive")
	}

	fmt.Printf("pprof metrics fetch done, write to archive file %s\n", filePath)

	uploadResp, uploadCfg, err := maybeUploadPprofArchive(ctx, http.DefaultClient, p, filePath, len(groups))
	if err != nil {
		return err
	}
	if uploadCfg == nil || uploadCfg.URL == "" {
		fmt.Println("beacon upload skipped: BW_BEACON_URL/BEACON_URL not set")
		return nil
	}

	fmt.Printf("beacon upload created: id=%s detail=%s/api/uploads/%s\n", uploadResp.ID, uploadCfg.URL, uploadResp.ID)

	return nil
}

func buildPprofArchiveEntryName(serverName string, serverID int64, pprofType string) string {
	base := fmt.Sprintf("%s_%d_%s", serverName, serverID, pprofType)
	if strings.HasSuffix(base, "_profile") {
		return base
	}
	return base + "_profile"
}

func resolveBeaconUploadConfig(p *PprofParam, sessionCount int, archiveFile string) (beaconUploadConfig, error) {
	instanceID := strings.TrimSpace(os.Getenv("MILVUS_INSTANCE_ID"))
	region := strings.TrimSpace(os.Getenv("REGION"))
	_ = sessionCount
	_ = archiveFile

	metaOverride := strings.TrimSpace(firstNonEmpty(p.BeaconMeta, os.Getenv("BW_BEACON_META"), os.Getenv("BEACON_META")))
	meta, err := buildBeaconMeta(p.Type, instanceID, region, metaOverride)
	if err != nil {
		return beaconUploadConfig{}, err
	}

	cfg := beaconUploadConfig{
		URL:      strings.TrimRight(strings.TrimSpace(firstNonEmpty(p.BeaconURL, os.Getenv("BW_BEACON_URL"), os.Getenv("BEACON_URL"))), "/"),
		Username: "birdwatcher",
		Remark:   strings.TrimSpace(firstNonEmpty(p.BeaconRemark, os.Getenv("BW_BEACON_REMARK"), os.Getenv("BEACON_REMARK"), buildBeaconRemark(p.Type, instanceID, region))),
		Meta:     meta,
	}

	if cfg.URL == "" {
		return cfg, nil
	}

	if !json.Valid([]byte(cfg.Meta)) {
		return beaconUploadConfig{}, errors.New("invalid beacon meta JSON")
	}

	return cfg, nil
}

func maybeUploadPprofArchive(ctx context.Context, client *http.Client, p *PprofParam, filePath string, sessionCount int) (*beaconUploadResponse, *beaconUploadConfig, error) {
	cfg, err := resolveBeaconUploadConfig(p, sessionCount, filepath.Base(filePath))
	if err != nil {
		return nil, nil, err
	}
	if cfg.URL == "" {
		return nil, &cfg, nil
	}

	resp, err := uploadFileToBeacon(ctx, client, cfg, filePath)
	if err != nil {
		return nil, &cfg, errors.Wrapf(err, "failed to upload %s to beacon", filePath)
	}
	return resp, &cfg, nil
}

func uploadFileToBeacon(ctx context.Context, client *http.Client, cfg beaconUploadConfig, filePath string) (*beaconUploadResponse, error) {
	if client == nil {
		client = http.DefaultClient
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writer.WriteField("username", cfg.Username); err != nil {
		return nil, err
	}
	if err := writer.WriteField("remark", cfg.Remark); err != nil {
		return nil, err
	}
	if err := writer.WriteField("meta", cfg.Meta); err != nil {
		return nil, err
	}

	part, err := writer.CreateFormFile("file", filepath.Base(filePath))
	if err != nil {
		return nil, err
	}
	if _, err := io.Copy(part, file); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.URL+"/upload", &body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("beacon upload failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	var parsed beaconUploadResponse
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return nil, err
	}
	return &parsed, nil
}

func collectPprofArchiveWriteError(signal <-chan error) error {
	var firstErr error
	for err := range signal {
		if err == nil {
			continue
		}
		fmt.Println("failed to write pprof:", err.Error())
		if firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func buildBeaconRemark(pprofType, instanceID, region string) string {
	parts := []string{"BW", "pprof", pprofType}
	if instanceID != "" {
		parts = append(parts, instanceID)
	}
	if region != "" {
		parts = append(parts, region)
	}
	return strings.Join(parts, " ")
}

func buildBeaconMeta(pprofType, instanceID, region, overrideJSON string) (string, error) {
	payload := map[string]any{
		"source":     "birdwatcher",
		"pprof_type": pprofType,
	}
	if instanceID != "" {
		payload["instance_id"] = instanceID
	}
	if region != "" {
		payload["region"] = region
	}

	if overrideJSON != "" {
		var overrides map[string]any
		if err := json.Unmarshal([]byte(overrideJSON), &overrides); err != nil {
			return "", errors.New("invalid beacon meta JSON")
		}
		for key, value := range overrides {
			payload[key] = value
		}
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func closePprofArchive(tw *tar.Writer, gw *gzip.Writer, f *os.File) error {
	if err := tw.Close(); err != nil {
		return err
	}
	if err := gw.Close(); err != nil {
		return err
	}
	return f.Close()
}
