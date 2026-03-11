package states

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveBeaconUploadConfig(t *testing.T) {
	t.Setenv("BW_BEACON_URL", "http://env-bw.example")
	t.Setenv("BEACON_URL", "http://env.example")
	t.Setenv("BW_BEACON_REMARK", "env-remark")
	t.Setenv("BW_BEACON_META", `{"source":"env","extra":"env"}`)

	p := &PprofParam{
		Type:         "goroutine",
		BeaconURL:    "http://flag.example",
		BeaconRemark: "flag-remark",
		BeaconMeta:   `{"source":"flag","override":"yes"}`,
	}

	cfg, err := resolveBeaconUploadConfig(p, 3, "bw_pprof_goroutine.tar.gz")
	require.NoError(t, err)
	require.Equal(t, "http://flag.example", cfg.URL)
	require.Equal(t, "birdwatcher", cfg.Username)
	require.Equal(t, "flag-remark", cfg.Remark)
	require.JSONEq(t, `{"source":"flag","pprof_type":"goroutine","override":"yes"}`, cfg.Meta)
}

func TestResolveBeaconUploadConfigBuildsDefaultMeta(t *testing.T) {
	t.Setenv("MILVUS_INSTANCE_ID", "milvus-dev-01")
	t.Setenv("REGION", "us-west-2")

	p := &PprofParam{Type: "heap"}

	cfg, err := resolveBeaconUploadConfig(p, 2, "bw_pprof_heap.tar.gz")
	require.NoError(t, err)
	require.Empty(t, cfg.URL)
	require.Equal(t, "birdwatcher", cfg.Username)
	require.Equal(t, "BW pprof heap milvus-dev-01 us-west-2", cfg.Remark)

	var meta map[string]any
	require.NoError(t, json.Unmarshal([]byte(cfg.Meta), &meta))
	require.Equal(t, "birdwatcher", meta["source"])
	require.Equal(t, "heap", meta["pprof_type"])
	require.Equal(t, "milvus-dev-01", meta["instance_id"])
	require.Equal(t, "us-west-2", meta["region"])
	_, hasSessionCount := meta["session_count"]
	require.False(t, hasSessionCount)
	_, hasArchiveFile := meta["archive_file"]
	require.False(t, hasArchiveFile)
}

func TestResolveBeaconUploadConfigRejectsInvalidMetaWhenBeaconEnabled(t *testing.T) {
	p := &PprofParam{
		Type:       "heap",
		BeaconURL:  "http://beacon.example",
		BeaconMeta: `{"broken":`,
	}

	_, err := resolveBeaconUploadConfig(p, 1, "bw_pprof_heap.tar.gz")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid beacon meta JSON")
}

func TestResolveBeaconUploadConfigOmitsEmptyInstanceAndRegion(t *testing.T) {
	p := &PprofParam{Type: "heap"}

	cfg, err := resolveBeaconUploadConfig(p, 1, "bw_pprof_heap.tar.gz")
	require.NoError(t, err)

	var meta map[string]any
	require.NoError(t, json.Unmarshal([]byte(cfg.Meta), &meta))
	_, hasInstanceID := meta["instance_id"]
	require.False(t, hasInstanceID)
	_, hasRegion := meta["region"]
	require.False(t, hasRegion)
}

func TestUploadFileToBeaconSendsExpectedMultipartFields(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "bw_pprof_goroutine.260310-120000.tar.gz")
	require.NoError(t, os.WriteFile(filePath, []byte("payload"), 0o600))

	var gotFileName string
	var gotFileBody string
	var gotUsername string
	var gotRemark string
	var gotMeta string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.NoError(t, r.ParseMultipartForm(1<<20))

		file, header, err := r.FormFile("file")
		require.NoError(t, err)
		defer file.Close()

		body, err := io.ReadAll(file)
		require.NoError(t, err)

		gotFileName = header.Filename
		gotFileBody = string(body)
		gotUsername = r.FormValue("username")
		gotRemark = r.FormValue("remark")
		gotMeta = r.FormValue("meta")

		w.Header().Set("Content-Type", "application/json")
		_, err = io.WriteString(w, `{"id":"42","status":"pending"}`)
		require.NoError(t, err)
	}))
	defer srv.Close()

	cfg := beaconUploadConfig{
		URL:      srv.URL,
		Username: "tester",
		Remark:   "remark",
		Meta:     `{"kind":"pprof"}`,
	}

	resp, err := uploadFileToBeacon(context.Background(), http.DefaultClient, cfg, filePath)
	require.NoError(t, err)
	require.Equal(t, "42", resp.ID)
	require.Equal(t, "pending", resp.Status)
	require.Equal(t, filepath.Base(filePath), gotFileName)
	require.Equal(t, "payload", gotFileBody)
	require.Equal(t, "tester", gotUsername)
	require.Equal(t, "remark", gotRemark)
	require.JSONEq(t, `{"kind":"pprof"}`, gotMeta)
}

func TestUploadFileToBeaconReturnsErrorOnNon200(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "bw_pprof_goroutine.260310-120000.tar.gz")
	require.NoError(t, os.WriteFile(filePath, []byte("payload"), 0o600))

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusBadGateway)
	}))
	defer srv.Close()

	cfg := beaconUploadConfig{
		URL:      srv.URL,
		Username: "tester",
		Remark:   "remark",
		Meta:     `{"kind":"pprof"}`,
	}

	_, err := uploadFileToBeacon(context.Background(), http.DefaultClient, cfg, filePath)
	require.Error(t, err)
	require.Contains(t, err.Error(), "status=502")
}

func TestBuildPprofArchiveEntryName(t *testing.T) {
	name := buildPprofArchiveEntryName("querynode", 12, "goroutine")
	require.Equal(t, "querynode_12_goroutine_profile", name)
	require.True(t, strings.HasSuffix(name, "_profile"))

	cpuName := buildPprofArchiveEntryName("querynode", 12, "profile")
	require.Equal(t, "querynode_12_profile", cpuName)
}

func TestMaybeUploadPprofArchiveSkipsWithoutBeaconURL(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "bw_pprof_heap.260310-120000.tar.gz")
	require.NoError(t, os.WriteFile(filePath, []byte("payload"), 0o600))

	resp, cfg, err := maybeUploadPprofArchive(context.Background(), http.DefaultClient, &PprofParam{Type: "heap"}, filePath, 1)
	require.NoError(t, err)
	require.Nil(t, resp)
	require.NotNil(t, cfg)
	require.Empty(t, cfg.URL)
}

func TestCollectPprofArchiveWriteErrorReturnsFirstError(t *testing.T) {
	ch := make(chan error, 3)
	first := errors.New("first")
	second := errors.New("second")
	ch <- nil
	ch <- first
	ch <- second
	close(ch)

	err := collectPprofArchiveWriteError(ch)
	require.ErrorIs(t, err, first)
}
