// Package metrics exposes Prometheus instrumentation for the blobscan-ipld
// service. It combines two kinds of series:
//
//   - Live in-process instruments (counters/histograms) updated by the pipeline
//     as blobs and epochs flow through it. These reset when the process restarts.
//   - Scrape-time gauges (epoch/blob totals, gaps, cursors, IPFS repo size) that
//     reflect durable state and are refreshed on every /metrics scrape via an
//     injected snapshot function.
//
// To avoid import cycles, this package does not depend on db, ipfs, or beacon.
// Callers map their own domain structs into DBSnapshot / IPFSSnapshot / the
// beacon counters carried by Snapshot.
package metrics

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const namespace = "blobscanipld"

// Metrics holds every live instrument plus the private registry they are
// registered on. Construct it with New. All record/observe helpers are safe to
// call on a nil *Metrics, so callers can leave metrics disabled by passing nil.
type Metrics struct {
	reg *prometheus.Registry

	blobsProcessed  *prometheus.CounterVec   // labels: network, source
	epochsProcessed *prometheus.CounterVec   // labels: network, source, result
	epochBuild      *prometheus.HistogramVec // labels: network
	ipfsUpload      prometheus.Histogram
	blocksUploaded  prometheus.Counter
	ipfsPins        *prometheus.CounterVec // labels: result
	networkRoot     prometheus.Histogram
	httpDuration    *prometheus.HistogramVec // labels: method, path, status
}

// New creates a Metrics with all instruments registered on a private registry,
// alongside the standard Go runtime and process collectors.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	m := &Metrics{
		reg: reg,
		blobsProcessed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "blobs_processed_total",
			Help:      "Total blobs processed by the pipeline, by network and source (live/backfill/push/backfill-ipfs).",
		}, []string{"network", "source"}),
		epochsProcessed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "epochs_processed_total",
			Help:      "Total epochs processed, by network, source and result (built/empty/skipped/failed).",
		}, []string{"network", "source", "result"}),
		epochBuild: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "epoch_build_duration_seconds",
			Help:      "Time to fetch and build an EpochNode (excludes IPFS upload).",
			Buckets:   prometheus.DefBuckets,
		}, []string{"network"}),
		ipfsUpload: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "ipfs_upload_duration_seconds",
			Help:      "Time to upload one blockstore (epoch or blob) to IPFS.",
			Buckets:   prometheus.DefBuckets,
		}),
		blocksUploaded: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "ipfs_blocks_uploaded_total",
			Help:      "Total IPLD blocks uploaded to IPFS.",
		}),
		ipfsPins: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "ipfs_pin_total",
			Help:      "Epoch pin attempts, by result (ok/failed/skipped).",
		}, []string{"result"}),
		networkRoot: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "network_root_rebuild_duration_seconds",
			Help:      "Time to rebuild and upload the NetworkRoot.",
			Buckets:   prometheus.DefBuckets,
		}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "http_request_duration_seconds",
			Help:      "API request latency, by method, route and status.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"method", "path", "status"}),
	}

	reg.MustRegister(
		m.blobsProcessed,
		m.epochsProcessed,
		m.epochBuild,
		m.ipfsUpload,
		m.blocksUploaded,
		m.ipfsPins,
		m.networkRoot,
		m.httpDuration,
	)

	return m
}

// Registry exposes the private registry so callers can register additional
// collectors (e.g. the scrape-time snapshot collector).
func (m *Metrics) Registry() *prometheus.Registry {
	if m == nil {
		return nil
	}
	return m.reg
}

// Handler returns the HTTP handler that serves the metrics in Prometheus text
// exposition format.
func (m *Metrics) Handler() http.Handler {
	if m == nil {
		return nil
	}
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// ─── Live record/observe helpers (nil-safe) ─────────────────────────────────

// RecordBlob counts one processed blob for the given network and source
// (live, backfill, push, backfill_ipfs).
func (m *Metrics) RecordBlob(network, source string) {
	if m == nil {
		return
	}
	m.blobsProcessed.WithLabelValues(network, source).Inc()
}

// RecordBlobs counts n processed blobs at once.
func (m *Metrics) RecordBlobs(network, source string, n int) {
	if m == nil || n <= 0 {
		return
	}
	m.blobsProcessed.WithLabelValues(network, source).Add(float64(n))
}

// RecordEpoch counts one processed epoch with the given result
// (built, empty, skipped, failed).
func (m *Metrics) RecordEpoch(network, source, result string) {
	if m == nil {
		return
	}
	m.epochsProcessed.WithLabelValues(network, source, result).Inc()
}

// ObserveEpochBuild records the time taken to build one epoch.
func (m *Metrics) ObserveEpochBuild(network string, d time.Duration) {
	if m == nil {
		return
	}
	m.epochBuild.WithLabelValues(network).Observe(d.Seconds())
}

// ObserveIPFSUpload records the time taken to upload one blockstore and the
// number of blocks uploaded.
func (m *Metrics) ObserveIPFSUpload(d time.Duration, blocks int) {
	if m == nil {
		return
	}
	m.ipfsUpload.Observe(d.Seconds())
	if blocks > 0 {
		m.blocksUploaded.Add(float64(blocks))
	}
}

// RecordPin counts one pin attempt by result (ok/failed/skipped).
func (m *Metrics) RecordPin(result string) {
	if m == nil {
		return
	}
	m.ipfsPins.WithLabelValues(result).Inc()
}

// ObserveNetworkRootRebuild records the time taken to rebuild the NetworkRoot.
func (m *Metrics) ObserveNetworkRootRebuild(d time.Duration) {
	if m == nil {
		return
	}
	m.networkRoot.Observe(d.Seconds())
}

// ObserveHTTPRequest records one API request's latency.
func (m *Metrics) ObserveHTTPRequest(method, path, status string, d time.Duration) {
	if m == nil {
		return
	}
	m.httpDuration.WithLabelValues(method, path, status).Observe(d.Seconds())
}

// ─── Scrape-time snapshot collector ─────────────────────────────────────────

// DBSnapshot carries the durable epoch/blob state read from Postgres at scrape
// time. It mirrors the fields of db.SummaryStats without importing db.
type DBSnapshot struct {
	Network        string
	EpochsIndexed  int64
	EmptyEpochs    int64
	BlobsIndexed   int64
	SizeBytes      int64
	FirstEpoch     uint64
	LastEpoch      uint64
	GapCount       int64
	LiveCursor     uint64
	BackfillCursor uint64
}

// IPFSSnapshot carries the IPFS repo storage numbers read at scrape time.
type IPFSSnapshot struct {
	RepoSizeBytes   uint64
	StorageMaxBytes uint64
}

// Snapshot is the full scrape-time view returned by the SnapshotFunc. The Has*
// flags indicate whether the corresponding backend was reachable so the
// collector can emit up/down series and skip stale gauges.
type Snapshot struct {
	DB      DBSnapshot
	HasDB   bool
	IPFS    IPFSSnapshot
	HasIPFS bool

	BeaconRPCRequests int64
	Beacon429s        int64
	HasBeacon         bool
}

// SnapshotFunc collects the current durable state. It is called on every scrape
// with a bounded context.
type SnapshotFunc func(ctx context.Context) Snapshot

// snapshotCollector is a prometheus.Collector that queries durable state on
// each scrape and emits it as gauges. Implementing Collect directly (rather
// than mutating registered Gauges) means the values are always fresh and never
// go stale between scrapes.
type snapshotCollector struct {
	snap SnapshotFunc

	epochsIndexed  *prometheus.Desc
	emptyEpochs    *prometheus.Desc
	blobsIndexed   *prometheus.Desc
	sizeBytes      *prometheus.Desc
	firstEpoch     *prometheus.Desc
	lastEpoch      *prometheus.Desc
	gapCount       *prometheus.Desc
	liveCursor     *prometheus.Desc
	backfillCursor *prometheus.Desc
	repoSize       *prometheus.Desc
	storageMax     *prometheus.Desc
	beaconRequests *prometheus.Desc
	beacon429s     *prometheus.Desc
	scrapeDBUp     *prometheus.Desc
	scrapeIPFSUp   *prometheus.Desc
}

func fqName(name string) string { return namespace + "_" + name }

// RegisterSnapshot builds the scrape-time collector from snap and registers it
// on the metrics registry. It is a no-op on a nil *Metrics or nil snap.
func (m *Metrics) RegisterSnapshot(snap SnapshotFunc) {
	if m == nil || snap == nil {
		return
	}
	networkLabel := []string{"network"}
	c := &snapshotCollector{
		snap:           snap,
		epochsIndexed:  prometheus.NewDesc(fqName("epochs_indexed"), "Epochs indexed in the database.", networkLabel, nil),
		emptyEpochs:    prometheus.NewDesc(fqName("empty_epochs"), "Indexed epochs with zero blobs.", networkLabel, nil),
		blobsIndexed:   prometheus.NewDesc(fqName("blobs_indexed_total"), "Total blobs indexed in the database.", networkLabel, nil),
		sizeBytes:      prometheus.NewDesc(fqName("indexed_size_bytes"), "Approximate total size of indexed blob data in bytes.", networkLabel, nil),
		firstEpoch:     prometheus.NewDesc(fqName("first_epoch"), "Lowest indexed epoch number.", networkLabel, nil),
		lastEpoch:      prometheus.NewDesc(fqName("last_epoch"), "Highest indexed epoch number.", networkLabel, nil),
		gapCount:       prometheus.NewDesc(fqName("epoch_gap_count"), "Number of missing epochs between first and last indexed.", networkLabel, nil),
		liveCursor:     prometheus.NewDesc(fqName("live_cursor"), "Live processing cursor (last epoch processed forward).", networkLabel, nil),
		backfillCursor: prometheus.NewDesc(fqName("backfill_cursor"), "Backfill cursor (last historical epoch processed).", networkLabel, nil),
		repoSize:       prometheus.NewDesc(fqName("ipfs_repo_size_bytes"), "IPFS repository size in bytes.", nil, nil),
		storageMax:     prometheus.NewDesc(fqName("ipfs_storage_max_bytes"), "IPFS configured maximum storage in bytes.", nil, nil),
		beaconRequests: prometheus.NewDesc(fqName("beacon_rpc_requests_total"), "Total RPC requests made to the beacon node.", nil, nil),
		beacon429s:     prometheus.NewDesc(fqName("beacon_rpc_429_total"), "Total HTTP 429 (rate limited) responses from the beacon node.", nil, nil),
		scrapeDBUp:     prometheus.NewDesc(fqName("scrape_db_up"), "1 if the database was reachable during the last scrape, else 0.", nil, nil),
		scrapeIPFSUp:   prometheus.NewDesc(fqName("scrape_ipfs_up"), "1 if the IPFS node was reachable during the last scrape, else 0.", nil, nil),
	}
	m.reg.MustRegister(c)
}

func (c *snapshotCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.epochsIndexed
	ch <- c.emptyEpochs
	ch <- c.blobsIndexed
	ch <- c.sizeBytes
	ch <- c.firstEpoch
	ch <- c.lastEpoch
	ch <- c.gapCount
	ch <- c.liveCursor
	ch <- c.backfillCursor
	ch <- c.repoSize
	ch <- c.storageMax
	ch <- c.beaconRequests
	ch <- c.beacon429s
	ch <- c.scrapeDBUp
	ch <- c.scrapeIPFSUp
}

func (c *snapshotCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s := c.snap(ctx)

	if s.HasDB {
		net := s.DB.Network
		g := func(desc *prometheus.Desc, v float64) {
			ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, v, net)
		}
		g(c.epochsIndexed, float64(s.DB.EpochsIndexed))
		g(c.emptyEpochs, float64(s.DB.EmptyEpochs))
		g(c.blobsIndexed, float64(s.DB.BlobsIndexed))
		g(c.sizeBytes, float64(s.DB.SizeBytes))
		g(c.firstEpoch, float64(s.DB.FirstEpoch))
		g(c.lastEpoch, float64(s.DB.LastEpoch))
		g(c.gapCount, float64(s.DB.GapCount))
		g(c.liveCursor, float64(s.DB.LiveCursor))
		g(c.backfillCursor, float64(s.DB.BackfillCursor))
		ch <- prometheus.MustNewConstMetric(c.scrapeDBUp, prometheus.GaugeValue, 1)
	} else {
		ch <- prometheus.MustNewConstMetric(c.scrapeDBUp, prometheus.GaugeValue, 0)
	}

	if s.HasIPFS {
		ch <- prometheus.MustNewConstMetric(c.repoSize, prometheus.GaugeValue, float64(s.IPFS.RepoSizeBytes))
		ch <- prometheus.MustNewConstMetric(c.storageMax, prometheus.GaugeValue, float64(s.IPFS.StorageMaxBytes))
		ch <- prometheus.MustNewConstMetric(c.scrapeIPFSUp, prometheus.GaugeValue, 1)
	} else {
		ch <- prometheus.MustNewConstMetric(c.scrapeIPFSUp, prometheus.GaugeValue, 0)
	}

	if s.HasBeacon {
		ch <- prometheus.MustNewConstMetric(c.beaconRequests, prometheus.CounterValue, float64(s.BeaconRPCRequests))
		ch <- prometheus.MustNewConstMetric(c.beacon429s, prometheus.CounterValue, float64(s.Beacon429s))
	}
}
