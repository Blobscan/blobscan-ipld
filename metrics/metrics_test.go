package metrics

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestHandlerExposesLiveAndSnapshot records a few live values and registers a
// snapshot func, then scrapes the handler and asserts the expected series and
// up flags appear. This also proves the exposition format parses (promhttp
// would 500 otherwise).
func TestHandlerExposesLiveAndSnapshot(t *testing.T) {
	m := New()
	m.RecordBlobs("mainnet", "backfill", 6)
	m.RecordEpoch("mainnet", "backfill", "built")
	m.ObserveEpochBuild("mainnet", 250*time.Millisecond)
	m.RecordPin("ok")

	m.RegisterSnapshot(func(context.Context) Snapshot {
		return Snapshot{
			HasDB: true,
			DB: DBSnapshot{
				Network:       "mainnet",
				EpochsIndexed: 42,
				BlobsIndexed:  100,
				GapCount:      3,
			},
			HasIPFS:           true,
			IPFS:              IPFSSnapshot{RepoSizeBytes: 1 << 30},
			HasBeacon:         true,
			BeaconRPCRequests: 999,
			Beacon429s:        7,
		}
	})

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))

	if rec.Code != 200 {
		t.Fatalf("metrics status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()

	for _, want := range []string{
		`blobscanipld_blobs_processed_total{network="mainnet",source="backfill"} 6`,
		`blobscanipld_epochs_processed_total{network="mainnet",result="built",source="backfill"} 1`,
		`blobscanipld_ipfs_pin_total{result="ok"} 1`,
		`blobscanipld_epochs_indexed{network="mainnet"} 42`,
		`blobscanipld_epoch_gap_count{network="mainnet"} 3`,
		`blobscanipld_ipfs_repo_size_bytes 1.073741824e+09`,
		`blobscanipld_beacon_rpc_requests_total 999`,
		`blobscanipld_beacon_rpc_429_total 7`,
		`blobscanipld_scrape_db_up 1`,
		`blobscanipld_scrape_ipfs_up 1`,
		// Go runtime collector is registered.
		`go_goroutines`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output missing %q", want)
		}
	}
}

// TestSnapshotDownFlags asserts the up flags report 0 when backends are absent.
func TestSnapshotDownFlags(t *testing.T) {
	m := New()
	m.RegisterSnapshot(func(context.Context) Snapshot { return Snapshot{} })

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))

	body := rec.Body.String()
	for _, want := range []string{
		`blobscanipld_scrape_db_up 0`,
		`blobscanipld_scrape_ipfs_up 0`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output missing %q", want)
		}
	}
}

// TestNilMetricsSafe verifies all helpers are safe on a nil *Metrics.
func TestNilMetricsSafe(t *testing.T) {
	var m *Metrics
	m.RecordBlob("n", "s")
	m.RecordBlobs("n", "s", 3)
	m.RecordEpoch("n", "s", "built")
	m.ObserveEpochBuild("n", time.Second)
	m.ObserveIPFSUpload(time.Second, 5)
	m.RecordPin("ok")
	m.ObserveNetworkRootRebuild(time.Second)
	m.ObserveHTTPRequest("GET", "/metrics", "200", time.Second)
	m.RegisterSnapshot(func(context.Context) Snapshot { return Snapshot{} })
	if m.Handler() != nil {
		t.Fatal("nil Metrics Handler should be nil")
	}
}
