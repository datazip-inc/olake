package destinationtest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/testutils/testhandler"
)

const (
	writerJarName = "olake-iceberg-java-writer.jar"
	// The Iceberg writer listens here and kills any other listener there.
	icebergWriterPort = 50051
	pollInterval      = 250 * time.Millisecond
)

// httpClient bounds every request, so a server that stalls fails the call instead of hanging the test.
var httpClient = &http.Client{Timeout: 30 * time.Second}

// icebergKind writes Iceberg tables through one catalog into the MinIO warehouse.
type icebergKind struct {
	name string
	// catalog is the container that serves the catalog.
	catalog *testhandler.Service
	// warehouse is the writer's iceberg_s3_path.
	warehouse string
	// config holds the catalog's own keys of the writer config.
	config map[string]any
	// dropNamespace removes a namespace and its tables from the catalog.
	dropNamespace func(ctx context.Context, namespace string) error
	// files is the key prefix that holds a namespace's files in the bucket.
	files func(namespace string) string
}

func (k *icebergKind) Name() string {
	return k.name
}

func (k *icebergKind) Services() []*testhandler.Service {
	return []*testhandler.Service{minioService, k.catalog}
}

// Open takes this test's turn on the Iceberg writer and returns the MinIO warehouse plus the catalog's keys.
func (k *icebergKind) Open(ctx context.Context, t testing.TB) (*types.WriterConfig, error) {
	if err := leaseIcebergWriter(ctx, t); err != nil {
		return nil, err
	}
	config := map[string]any{
		"iceberg_s3_path": k.warehouse,
		"aws_region":      region,
		"aws_access_key":  serverUser,
		"aws_secret_key":  serverKey,
		"s3_endpoint":     "http://" + minioService.Addr(),
		"s3_path_style":   true,
		"s3_use_ssl":      false,
	}
	if jar := writerJar(); jar != "" {
		config["sink_jar_path"] = jar
	}
	maps.Copy(config, k.config)
	return &types.WriterConfig{Type: types.Iceberg, WriterConfig: config}, nil
}

func (k *icebergKind) Drop(ctx context.Context, database string) error {
	return errors.Join(k.dropNamespace(ctx, database), removePrefix(ctx, k.files(database)))
}

// icebergLease is this process's view of the machine-wide right to run the Iceberg writer.
var icebergLease struct {
	sync.Mutex
	// holder is the test that holds the lease; its subtests share it.
	holder string
	// leak is the test that left the writer running, which poisons this process's writer.
	leak string
}

// leaseIcebergWriter makes this test the only user of the Iceberg writer on the machine until it ends.
// The writer runs one JVM on a fixed port and kills any other listener there, so tests must take turns.
func leaseIcebergWriter(ctx context.Context, t testing.TB) error {
	name := t.Name()
	icebergLease.Lock()
	held := icebergLease.holder != "" && (name == icebergLease.holder || strings.HasPrefix(name, icebergLease.holder+"/"))
	icebergLease.Unlock()
	if held {
		return nil
	}
	release, err := lockFile(ctx, filepath.Join(os.TempDir(), "olake-testhandler-iceberg.lock"))
	if err != nil {
		return fmt.Errorf("no turn on the Iceberg writer, which one test at a time may use: %s", err)
	}
	icebergLease.Lock()
	leak := icebergLease.leak
	if leak == "" {
		icebergLease.holder = name
	}
	icebergLease.Unlock()
	if leak != "" {
		release()
		return fmt.Errorf("%s left the Iceberg writer running, so this process would reuse its catalog", leak)
	}
	// A listener that is already there is left over from an earlier run; the writer kills it when it starts.
	orphan := icebergWriterAnswers()
	t.Cleanup(func() {
		leaked := !orphan && !icebergWriterStopped()
		icebergLease.Lock()
		icebergLease.holder = ""
		if leaked {
			icebergLease.leak = name
		}
		icebergLease.Unlock()
		release()
		if leaked {
			t.Errorf("testhandler: port %d still answers after the test, so the Iceberg writer was left running; shut the writer pool down (a failed NewWriterPool leaves it running too)", icebergWriterPort)
		}
	})
	return nil
}

// icebergWriterAnswers reports whether something accepts connections on the writer's port.
func icebergWriterAnswers() bool {
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(testhandler.Host, strconv.Itoa(icebergWriterPort)), time.Second)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// icebergWriterStopped waits for the writer's port to stop answering.
func icebergWriterStopped() bool {
	deadline := time.Now().Add(15 * time.Second)
	for icebergWriterAnswers() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(pollInterval)
	}
	return true
}

// writerJar finds the built writer jar above the working directory. The writer's own lookup
// fails from some package directories; when no jar is found the writer reports that itself.
func writerJar() string {
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}
	for {
		jar := filepath.Join(dir, writerJarName)
		if _, err := os.Stat(jar); err == nil {
			return jar
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

// request sends one HTTP request and returns the status code and the body.
func request(ctx context.Context, method, url string, header map[string]string, body string) (int, string, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, strings.NewReader(body))
	if err != nil {
		return 0, "", err
	}
	for key, value := range header {
		req.Header.Set(key, value)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	return resp.StatusCode, string(data), err
}

// httpReady passes when the published port answers a GET on path with 200.
func httpReady(path string) func(context.Context, *testhandler.Service) error {
	return func(ctx context.Context, s *testhandler.Service) error {
		status, _, err := request(ctx, http.MethodGet, "http://"+s.Addr()+path, nil, "")
		if err == nil && status != http.StatusOK {
			err = fmt.Errorf("GET %s returned status %d", path, status)
		}
		return err
	}
}
