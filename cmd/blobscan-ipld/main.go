// Command blobscan-ipld is the main entry point for the Blobscan IPLD DAG
// generator. It exposes several subcommands for different operation modes.
package main

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/blobscan/blobscan-ipld/api"
	"github.com/blobscan/blobscan-ipld/builder"
	carexport "github.com/blobscan/blobscan-ipld/car"
	"github.com/blobscan/blobscan-ipld/config"
	"github.com/blobscan/blobscan-ipld/db"
	"github.com/blobscan/blobscan-ipld/generator"
	"github.com/blobscan/blobscan-ipld/ipfs"
	"github.com/blobscan/blobscan-ipld/store"
	"github.com/blobscan/blobscan-ipld/types"

	"flag"
	"runtime"
	"sync/atomic"

	sentry "github.com/getsentry/sentry-go"
	sentryslog "github.com/getsentry/sentry-go/slog"
	"github.com/ipfs/go-cid"
)

const usage = `blobscan-ipld — Ethereum blob data IPLD DAG generator

Usage:
  blobscan-ipld <subcommand> [flags]

Subcommands:
  run              Pull epochs from the beacon node and process them continuously
  serve            Start the HTTP push API (no beacon node required)
  epoch            Process a single epoch one-shot (beacon-pull mode)
  finalize-epoch   Build/update the EpochNode for an epoch whose blobs were pushed via API
  export-car       Export a CAR v2 file for a single epoch from the DB
  export-car-range Export a single CAR v2 file covering a range of epochs
  backfill-ipfs    Re-fetch blob data from beacon and upload historical epochs to IPFS
  pin-existing     Recursively pin epoch roots from the DB that are not yet pinned (GC protection)
  export-blob-refs Export blob CID references as CSV for import into blobscan DB
  summary          Show indexed-data statistics (use -help for detail flags)
  repair-epochs    Rebuild epoch nodes from DB cache for epochs saved with blob_count=0 by mistake
  health-check     Analyze DB integrity (blob_count/size/network/index invariants + offline CID recompute)

Global flags (before subcommand):
  -log-level <level>  Log level: debug, info, warn, error (default: info)

Environment variables:
  NETWORK_NAME, {MAINNET,SEPOLIA,HOODI}_BEACON_RPC, DATA_DIR, IPFS_API_ADDR, POSTGRES_DSN, ...
  See docs/configuration.md for the full list.

Examples:
  blobscan-ipld run
  blobscan-ipld serve
  blobscan-ipld -n 300000 epoch
  blobscan-ipld -n 300000 finalize-epoch
  blobscan-ipld -n 300000 -out /tmp/300000.car export-car
  blobscan-ipld -from 300000 -to 300099 -out /tmp/range.car export-car-range
  blobscan-ipld backfill-ipfs
  blobscan-ipld backfill-ipfs -from 300000 -to 300099
  blobscan-ipld pin-existing
  blobscan-ipld pin-existing -dry-run
  blobscan-ipld export-blob-refs -out /tmp/refs.csv
  blobscan-ipld export-blob-refs -from 300000 -to 300099
  blobscan-ipld export-blob-refs -meta -out /tmp/refs.csv
  blobscan-ipld summary
  blobscan-ipld summary -gaps -empty -top 10 -monthly -check-ipfs -ipfs-stat
  blobscan-ipld repair-epochs
  blobscan-ipld repair-epochs -dry-run
  blobscan-ipld repair-epochs -no-verify
  blobscan-ipld health-check
  blobscan-ipld health-check -tier 0
  blobscan-ipld health-check -tier 1 -samples 20
`

func main() {
	// Global flags parsed before the subcommand.
	globalFlags := flag.NewFlagSet("blobscan-ipld", flag.ExitOnError)
	logLevel := globalFlags.String("log-level", "info", "log level: debug, info, warn, error")
	globalFlags.Usage = func() { fmt.Fprint(os.Stderr, usage) }

	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(1)
	}

	// Consume global flags that appear before the subcommand.
	subArgs := os.Args[1:]
	_ = globalFlags.Parse(subArgs)
	subArgs = globalFlags.Args()
	if len(subArgs) == 0 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(1)
	}

	subcommand := subArgs[0]
	subArgs = subArgs[1:]

	log := newLogger(*logLevel)

	cfg, err := config.Load()
	if err != nil {
		log.Error("failed to load config", "err", err)
		os.Exit(1)
	}

	if cfg.Sentry.DSN != "" {
		if err := sentry.Init(sentry.ClientOptions{
			Dsn:              cfg.Sentry.DSN,
			Environment:      cfg.Sentry.Environment,
			Release:          cfg.Sentry.Release,
			TracesSampleRate: cfg.Sentry.SampleRate,
		}); err != nil {
			log.Warn("sentry init failed", "err", err)
		} else {
			log.Info("✓ Sentry error tracking enabled", "environment", cfg.Sentry.Environment)
			defer sentry.Flush(2 * time.Second)
			// Tee Error-level logs to Sentry automatically.
			sentryHandler := sentryslog.Option{
				EventLevel: []slog.Level{slog.LevelError},
				LogLevel:   []slog.Level{},
			}.NewSentryHandler(context.Background())
			log = slog.New(newTeeHandler(log.Handler(), sentryHandler))
		}
	}

	for _, dir := range []string{cfg.Storage.DataDir, cfg.Storage.CARDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Error("failed to create directory", "dir", dir, "err", err)
			os.Exit(1)
		}
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	switch subcommand {
	case "run":
		cmdRun(ctx, cfg, log, subArgs)
	case "serve":
		cmdServe(ctx, cfg, log, subArgs)
	case "epoch":
		cmdEpoch(ctx, cfg, log, subArgs)
	case "finalize-epoch":
		cmdFinalizeEpoch(ctx, cfg, log, subArgs)
	case "export-car":
		cmdExportCAR(ctx, cfg, log, subArgs)
	case "export-car-range":
		cmdExportCARRange(ctx, cfg, log, subArgs)
	case "backfill-ipfs":
		cmdBackfillIPFS(ctx, cfg, log, subArgs)
	case "pin-existing":
		cmdPinExisting(ctx, cfg, log, subArgs)
	case "export-blob-refs":
		cmdExportBlobRefs(ctx, cfg, log, subArgs)
	case "summary":
		cmdSummary(ctx, cfg, subArgs)
	case "repair-epochs":
		cmdRepairEpochs(ctx, cfg, log, subArgs)
	case "health-check", "check":
		cmdHealthCheck(ctx, cfg, log, subArgs)
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n\n%s", subcommand, usage)
		os.Exit(1)
	}
}

// ─── Subcommands ──────────────────────────────────────────────────────────────

// run: continuous beacon-pull loop.
func cmdRun(ctx context.Context, cfg *config.Config, log *slog.Logger, args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	_ = fs.Parse(args)

	gen, err := generator.New(ctx, cfg, log)
	if err != nil {
		log.Error("failed to create generator", "err", err)
		os.Exit(1)
	}
	defer gen.Close()

	if err := gen.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("generator exited with error", "err", err)
		os.Exit(1)
	}
}

// serve: start the HTTP push API; optionally also run the beacon-pull loop.
func cmdServe(ctx context.Context, cfg *config.Config, log *slog.Logger, args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	withPull := fs.Bool("pull", false, "also run the beacon-pull loop alongside the API")
	_ = fs.Parse(args)

	gen, err := generator.New(ctx, cfg, log)
	if err != nil {
		log.Error("failed to create generator", "err", err)
		os.Exit(1)
	}
	defer gen.Close()

	listen := cfg.Generator.APIListen
	if listen == "" {
		listen = ":8080"
	}

	srv := api.New(listen, gen.ProcessBlobInput, gen.FinalizeEpochWithCID, log)

	// Start API server in background.
	srvErr := make(chan error, 1)
	go func() { srvErr <- srv.ListenAndServe() }()

	if *withPull {
		// Run beacon-pull loop in background.
		go func() {
			if err := gen.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Error("beacon pull loop error", "err", err)
			}
		}()
	}

	select {
	case <-ctx.Done():
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*1e9)
		defer shutCancel()
		_ = srv.Shutdown(shutCtx)
	case err := <-srvErr:
		if err != nil {
			log.Error("api server error", "err", err)
			os.Exit(1)
		}
	}
}

// epoch: process a single epoch (beacon-pull, one-shot).
func cmdEpoch(ctx context.Context, cfg *config.Config, log *slog.Logger, args []string) {
	fs := flag.NewFlagSet("epoch", flag.ExitOnError)
	epochNum := fs.Uint64("n", 0, "epoch number to process (required)")
	_ = fs.Parse(args)

	if *epochNum == 0 {
		fmt.Fprintln(os.Stderr, "epoch: -n <epoch> is required")
		os.Exit(1)
	}

	gen, err := generator.New(ctx, cfg, log)
	if err != nil {
		log.Error("failed to create generator", "err", err)
		os.Exit(1)
	}
	defer gen.Close()

	if err := gen.ProcessSingleEpoch(ctx, *epochNum); err != nil {
		log.Error("epoch processing failed", "epoch", *epochNum, "err", err)
		os.Exit(1)
	}
	log.Info("epoch processed", "epoch", *epochNum)
}

// finalize-epoch: build/update the EpochNode for an epoch whose blobs were
// pushed via the HTTP API, then rebuild the NetworkRoot.
func cmdFinalizeEpoch(ctx context.Context, cfg *config.Config, log *slog.Logger, args []string) {
	fs := flag.NewFlagSet("finalize-epoch", flag.ExitOnError)
	epochNum := fs.Uint64("n", 0, "epoch number to finalize (required)")
	_ = fs.Parse(args)

	if *epochNum == 0 {
		fmt.Fprintln(os.Stderr, "finalize-epoch: -n <epoch> is required")
		os.Exit(1)
	}

	gen, err := generator.New(ctx, cfg, log)
	if err != nil {
		log.Error("failed to create generator", "err", err)
		os.Exit(1)
	}
	defer gen.Close()

	if err := gen.FinalizeEpoch(ctx, *epochNum); err != nil {
		log.Error("finalize epoch failed", "epoch", *epochNum, "err", err)
		os.Exit(1)
	}
	log.Info("epoch finalized", "epoch", *epochNum)
}

// export-car: fetch all blocks for an epoch from IPFS (via DB CIDs) and write
// a self-contained CAR v2 file. Does not require the beacon node.
func cmdExportCAR(ctx context.Context, cfg *config.Config, log *slog.Logger, args []string) {
	fs := flag.NewFlagSet("export-car", flag.ExitOnError)
	epochNum := fs.Uint64("n", 0, "epoch number to export (required)")
	outPath := fs.String("out", "", "output CAR file path (default: <car_dir>/<network>/<epoch>.car)")
	_ = fs.Parse(args)

	if *epochNum == 0 {
		fmt.Fprintln(os.Stderr, "export-car: -n <epoch> is required")
		os.Exit(1)
	}
	if cfg.Storage.PostgresDSN == "" {
		fmt.Fprintln(os.Stderr, "export-car: postgres_dsn is required (epoch CIDs are stored in the DB)")
		os.Exit(1)
	}

	dbClient, err := db.New(ctx, cfg.Storage.PostgresDSN)
	if err != nil {
		log.Error("failed to connect to postgres", "err", err)
		os.Exit(1)
	}
	defer dbClient.Close()

	// Load the epoch CID from the DB.
	records, err := dbClient.GetAllEpochs(ctx, cfg.Network.Name)
	if err != nil {
		log.Error("failed to query epochs", "err", err)
		os.Exit(1)
	}

	var epochCIDStr string
	for _, r := range records {
		if r.Epoch == *epochNum {
			epochCIDStr = r.CID
			break
		}
	}
	if epochCIDStr == "" {
		log.Error("epoch not found in database", "epoch", *epochNum)
		os.Exit(1)
	}

	epochCID, err := cid.Decode(epochCIDStr)
	if err != nil {
		log.Error("invalid epoch CID", "cid", epochCIDStr, "err", err)
		os.Exit(1)
	}

	// Load all blob CIDs for this epoch.
	blobs, err := dbClient.GetBlobsByEpoch(ctx, cfg.Network.Name, *epochNum)
	if err != nil {
		log.Error("failed to query blobs", "epoch", *epochNum, "err", err)
		os.Exit(1)
	}

	// Rebuild an in-memory blockstore by fetching blocks from IPFS.
	ipfsClient, err := newIPFSClientFromConfig(cfg)
	if err != nil {
		log.Error("failed to create ipfs client", "err", err)
		os.Exit(1)
	}

	bs := store.NewMemBlockstore()
	cids := make([]cid.Cid, 0, len(blobs)*2+1)
	cids = append(cids, epochCID)
	for _, b := range blobs {
		dc, _ := cid.Decode(b.DataCID)
		mc, _ := cid.Decode(b.MetaCID)
		cids = append(cids, dc, mc)
	}

	log.Info("fetching blocks from IPFS", "count", len(cids))
	if err := ipfsClient.GetBlocks(ctx, bs, cids); err != nil {
		log.Error("failed to fetch blocks from IPFS", "err", err)
		os.Exit(1)
	}

	path := *outPath
	if path == "" {
		path = carexport.EpochCARPath(cfg.Storage.CARDir, cfg.Network.Name, *epochNum)
	}

	if err := carexport.ExportRangeCAR(ctx, bs, epochCID, path); err != nil {
		log.Error("failed to export CAR", "err", err)
		os.Exit(1)
	}

	log.Info("CAR exported", "epoch", *epochNum, "path", path, "cid", epochCID)
}

// export-car-range: collect all blocks for a contiguous range of epochs and
// write them into a single CAR v2 file. The root of the file is a RangeNode
// that maps each epoch number to its EpochNode CID.
func cmdExportCARRange(ctx context.Context, cfg *config.Config, log *slog.Logger, args []string) {
	fs := flag.NewFlagSet("export-car-range", flag.ExitOnError)
	fromEpoch := fs.Uint64("from", 0, "first epoch in range (required)")
	toEpoch := fs.Uint64("to", 0, "last epoch in range (required)")
	outPath := fs.String("out", "", "output CAR file path (default: <car_dir>/<network>/<from>-<to>.car)")
	_ = fs.Parse(args)

	if *fromEpoch == 0 || *toEpoch == 0 {
		fmt.Fprintln(os.Stderr, "export-car-range: -from and -to are required")
		os.Exit(1)
	}
	if *toEpoch < *fromEpoch {
		fmt.Fprintln(os.Stderr, "export-car-range: -to must be >= -from")
		os.Exit(1)
	}
	if cfg.Storage.PostgresDSN == "" {
		fmt.Fprintln(os.Stderr, "export-car-range: postgres_dsn is required (epoch CIDs are stored in the DB)")
		os.Exit(1)
	}

	dbClient, err := db.New(ctx, cfg.Storage.PostgresDSN)
	if err != nil {
		log.Error("failed to connect to postgres", "err", err)
		os.Exit(1)
	}
	defer dbClient.Close()

	// Load all epoch records in the range from the DB.
	allRecords, err := dbClient.GetAllEpochs(ctx, cfg.Network.Name)
	if err != nil {
		log.Error("failed to query epochs", "err", err)
		os.Exit(1)
	}

	type epochEntry struct {
		record db.EpochRecord
		cid    cid.Cid
	}
	var entries []epochEntry
	for _, r := range allRecords {
		if r.Epoch >= *fromEpoch && r.Epoch <= *toEpoch {
			c, err := cid.Decode(r.CID)
			if err != nil {
				log.Error("invalid epoch CID", "epoch", r.Epoch, "cid", r.CID, "err", err)
				os.Exit(1)
			}
			entries = append(entries, epochEntry{record: r, cid: c})
		}
	}

	if len(entries) == 0 {
		log.Error("no epochs found in database for range", "from", *fromEpoch, "to", *toEpoch)
		os.Exit(1)
	}
	log.Info("epochs found", "count", len(entries), "from", *fromEpoch, "to", *toEpoch)

	ipfsClient, err := newIPFSClientFromConfig(cfg)
	if err != nil {
		log.Error("failed to create ipfs client", "err", err)
		os.Exit(1)
	}

	// Accumulate all CIDs to fetch: one epoch node + blob data + blob meta per blob.
	bs := store.NewMemBlockstore()
	var epochResults []types.EpochResult

	for _, e := range entries {
		blobs, err := dbClient.GetBlobsByEpoch(ctx, cfg.Network.Name, e.record.Epoch)
		if err != nil {
			log.Error("failed to query blobs", "epoch", e.record.Epoch, "err", err)
			os.Exit(1)
		}

		cids := make([]cid.Cid, 0, len(blobs)*2+1)
		cids = append(cids, e.cid)
		for _, b := range blobs {
			dc, _ := cid.Decode(b.DataCID)
			mc, _ := cid.Decode(b.MetaCID)
			cids = append(cids, dc, mc)
		}

		log.Info("fetching epoch blocks from IPFS", "epoch", e.record.Epoch, "blocks", len(cids))
		if err := ipfsClient.GetBlocks(ctx, bs, cids); err != nil {
			log.Error("failed to fetch blocks", "epoch", e.record.Epoch, "err", err)
			os.Exit(1)
		}

		epochResults = append(epochResults, types.EpochResult{
			Epoch:                e.record.Epoch,
			CID:                  e.cid,
			ApproximateSizeBytes: e.record.SizeBytes,
		})
	}

	// Build a RangeNode as the CAR root (stored into bs via the link system).
	lsys := store.NewLinkSystem(bs)
	rangeResult, err := builder.BuildRangeNode(ctx, lsys, cfg.Network.Name, epochResults)
	if err != nil {
		log.Error("failed to build range node", "err", err)
		os.Exit(1)
	}

	path := *outPath
	if path == "" {
		path = carexport.RangeCARPath(cfg.Storage.CARDir, cfg.Network.Name, *fromEpoch, *toEpoch)
	}

	if err := carexport.ExportRangeCAR(ctx, bs, rangeResult.CID, path); err != nil {
		log.Error("failed to export CAR", "err", err)
		os.Exit(1)
	}

	log.Info("range CAR exported",
		"from", *fromEpoch,
		"to", *toEpoch,
		"epochs", len(entries),
		"path", path,
		"root_cid", rangeResult.CID,
	)
}

// backfill-ipfs: re-fetch blob data from beacon and upload all IPLD blocks to
// IPFS for epochs that were indexed with skip_upload=true. Epochs whose CIDs
// match the DB values are uploaded without modifying the DB; mismatching CIDs
// are logged as errors and the DB is updated with the freshly-computed values.
func cmdBackfillIPFS(ctx context.Context, cfg *config.Config, log *slog.Logger, args []string) {
	fs := flag.NewFlagSet("backfill-ipfs", flag.ExitOnError)
	fromEpoch := fs.Uint64("from", 0, "first epoch to backfill (default: start_epoch from config)")
	toEpoch := fs.Uint64("to", 0, "last epoch to backfill (default: highest epoch in DB)")
	_ = fs.Parse(args)

	if cfg.Storage.PostgresDSN == "" {
		fmt.Fprintln(os.Stderr, "backfill-ipfs: postgres_dsn is required")
		os.Exit(1)
	}
	if cfg.IPFS.SkipUpload {
		fmt.Fprintln(os.Stderr, "backfill-ipfs: ipfs.skip_upload must be false")
		os.Exit(1)
	}
	if cfg.Network.BeaconRPC == "" {
		fmt.Fprintln(os.Stderr, "backfill-ipfs: beacon_rpc is required to re-fetch blob data")
		os.Exit(1)
	}

	gen, err := generator.New(ctx, cfg, log)
	if err != nil {
		log.Error("failed to create generator", "err", err)
		os.Exit(1)
	}
	defer gen.Close()

	from := *fromEpoch
	if from == 0 {
		from = cfg.Generator.StartEpoch
	}

	to := *toEpoch
	if to == 0 {
		// Default: process all epochs stored in the DB.
		dbClient, err := db.New(ctx, cfg.Storage.PostgresDSN)
		if err != nil {
			log.Error("failed to connect to postgres", "err", err)
			os.Exit(1)
		}
		maxEpoch, err := dbClient.GetMaxEpoch(ctx, cfg.Network.Name)
		dbClient.Close()
		if err != nil {
			log.Error("failed to query max epoch", "err", err)
			os.Exit(1)
		}
		if maxEpoch == 0 {
			log.Info("no epochs in DB, nothing to backfill")
			return
		}
		to = maxEpoch
	}

	if err := gen.BackfillIPFS(ctx, from, to); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("backfill-ipfs failed", "err", err)
		os.Exit(1)
	}
}

// pin-existing: recursively pin every epoch root recorded in the DB that is not
// already pinned. Use this once after enabling pinning to protect epochs that
// were uploaded before IPFS_PIN_ON_ADD defaulted to true (their blocks are in
// the datastore but unpinned, so `ipfs repo gc` could collect them). It only
// pins roots already present on the node; it does not re-upload missing data —
// use backfill-ipfs for that.
func cmdPinExisting(ctx context.Context, cfg *config.Config, log *slog.Logger, args []string) {
	fs := flag.NewFlagSet("pin-existing", flag.ExitOnError)
	dryRun := fs.Bool("dry-run", false, "report what would be pinned without pinning")
	workersFlag := fs.Int("workers", 4, "parallel pin/add requests (recursive pinning is heavy; keep low to avoid saturating the node)")
	timeoutFlag := fs.Duration("pin-timeout", 2*time.Minute, "per-pin timeout; a recursive pin/add exceeding this is retried")
	retriesFlag := fs.Int("retries", 2, "retry attempts per epoch on transient pin failure")
	limitFlag := fs.Int("limit", 0, "only attempt the first N not-yet-pinned epochs (0 = all); useful for testing")
	_ = fs.Parse(args)

	workers := *workersFlag
	if workers <= 0 {
		workers = 1
	}
	pinTimeout := *timeoutFlag
	pinRetries := *retriesFlag
	if pinRetries < 0 {
		pinRetries = 0
	}

	if cfg.Storage.PostgresDSN == "" {
		fmt.Fprintln(os.Stderr, "pin-existing: postgres_dsn is required")
		os.Exit(1)
	}
	if cfg.IPFS.SkipUpload || cfg.IPFS.APIAddr == "" {
		fmt.Fprintln(os.Stderr, "pin-existing: IPFS must be configured (skip_upload=false and api_addr set)")
		os.Exit(1)
	}

	dbClient, err := db.New(ctx, cfg.Storage.PostgresDSN)
	if err != nil {
		log.Error("failed to connect to postgres", "err", err)
		os.Exit(1)
	}
	defer dbClient.Close()

	records, err := dbClient.GetAllEpochs(ctx, cfg.Network.Name)
	if err != nil {
		log.Error("failed to load epochs", "err", err)
		os.Exit(1)
	}
	if len(records) == 0 {
		log.Info("no epochs in DB, nothing to pin")
		return
	}

	ipfsClient, err := newIPFSClientFromConfig(cfg)
	if err != nil {
		log.Error("failed to create IPFS client", "err", err)
		os.Exit(1)
	}

	// Fetch the current pin set once so already-pinned epochs are skipped
	// cheaply rather than issuing a redundant pin/add for each.
	pins, err := ipfsClient.ListRecursivePins(ctx)
	if err != nil {
		log.Warn("could not list existing pins; will attempt to pin every epoch", "err", err)
		pins = map[string]struct{}{}
	}

	// Collect the epochs that still need pinning.
	type pinJob struct {
		epoch uint64
		cid   cid.Cid
	}
	var todo []pinJob
	badCIDs := 0
	for _, rec := range records {
		c, err := cid.Decode(rec.CID)
		if err != nil {
			badCIDs++
			continue
		}
		if _, ok := pins[c.String()]; ok {
			continue
		}
		todo = append(todo, pinJob{epoch: rec.Epoch, cid: c})
	}

	alreadyPinned := len(records) - len(todo) - badCIDs
	log.Info("pin-existing scan complete",
		"epochs", len(records),
		"already_pinned", alreadyPinned,
		"to_pin", len(todo),
		"unparseable_cids", badCIDs)

	if *limitFlag > 0 && *limitFlag < len(todo) {
		todo = todo[:*limitFlag]
		log.Info("limiting this run", "attempting", len(todo))
	}

	if *dryRun {
		log.Info("dry-run: no pins performed")
		return
	}
	if len(todo) == 0 {
		log.Info("all epochs already pinned")
		return
	}

	// Pin in parallel. Recursive pin/add is heavier than block upload (Kubo
	// walks the DAG), so default to a gentler concurrency than UploadWorkers
	// to avoid saturating the node; override with -workers.
	jobs := make(chan pinJob, len(todo))
	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		pinned int
		failed int
		done   int
	)
	total := len(todo)
	var (
		failedEpochs []uint64 // epochs whose pin failed after retries; re-run to retry them
		sampleErr    error    // first failure, surfaced so the cause is visible
	)
	report := func() {
		fmt.Fprintf(os.Stderr, "\r  pinning: %d/%d  (%.0f%%, %d failed)   ",
			done, total, float64(done)/float64(total)*100, failed)
	}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				err := pinWithRetry(ctx, ipfsClient, j.cid, pinTimeout, pinRetries)
				mu.Lock()
				done++
				if err != nil {
					failed++
					if sampleErr == nil {
						sampleErr = err
					}
					failedEpochs = append(failedEpochs, j.epoch)
				} else {
					pinned++
				}
				report()
				mu.Unlock()
			}
		}()
	}
	for _, j := range todo {
		jobs <- j
	}
	close(jobs)
	wg.Wait()
	fmt.Fprintln(os.Stderr)

	if len(failedEpochs) > 0 {
		sortUint64(failedEpochs)
		log.Warn("some epochs could not be pinned; re-run pin-existing to retry them",
			"count", len(failedEpochs), "first", firstN(failedEpochs, 10))
	}
	if sampleErr != nil {
		log.Warn("sample pin failure (first error encountered)", "err", sampleErr)
	}
	log.Info("pin-existing complete", "pinned", pinned, "failed", failed)
	if failed > 0 {
		os.Exit(1)
	}
}

// pinWithRetry pins cid, retrying transient failures with a fresh per-attempt
// deadline. Recursive pin/add has no client-side timeout (see ipfs.Client), so
// each attempt is bounded by perAttempt to avoid hanging on a stuck pin.
func pinWithRetry(ctx context.Context, ipfsClient *ipfs.Client, c cid.Cid, perAttempt time.Duration, retries int) error {
	var err error
	for attempt := 0; attempt <= retries; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		actx, cancel := context.WithTimeout(ctx, perAttempt)
		err = ipfsClient.Pin(actx, c)
		cancel()
		if err == nil {
			return nil
		}
	}
	return err
}

// firstN returns up to n elements of s, for compact log output.
func firstN(s []uint64, n int) []uint64 {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// export-blob-refs: export blob CID references as CSV importable into blobscan's
// blob_data_storage_reference table.
func cmdExportBlobRefs(ctx context.Context, cfg *config.Config, log *slog.Logger, args []string) {
	fs := flag.NewFlagSet("export-blob-refs", flag.ExitOnError)
	fromEpoch := fs.Uint64("from", 0, "first epoch to export (default: 0)")
	toEpoch := fs.Uint64("to", 0, "last epoch to export (default: max epoch in DB)")
	outPath := fs.String("out", "", "output CSV file path (default: stdout)")
	includeMeta := fs.Bool("meta", false, "include meta_reference column in output")
	_ = fs.Parse(args)

	if cfg.Storage.PostgresDSN == "" {
		fmt.Fprintln(os.Stderr, "export-blob-refs: POSTGRES_DSN is required")
		os.Exit(1)
	}

	dbClient, err := db.New(ctx, cfg.Storage.PostgresDSN)
	if err != nil {
		log.Error("failed to connect to postgres", "err", err)
		os.Exit(1)
	}
	defer dbClient.Close()

	// Default -to to the max epoch in the DB when not specified.
	if *toEpoch == 0 {
		maxEpoch, err := dbClient.GetMaxEpoch(ctx, cfg.Network.Name)
		if err != nil {
			log.Error("failed to query max epoch", "err", err)
			os.Exit(1)
		}
		*toEpoch = maxEpoch
	}

	refs, err := dbClient.GetBlobRefs(ctx, cfg.Network.Name, *fromEpoch, *toEpoch)
	if err != nil {
		log.Error("failed to query blob refs", "err", err)
		os.Exit(1)
	}

	// Determine output writer.
	var out *os.File
	if *outPath != "" {
		out, err = os.Create(*outPath)
		if err != nil {
			log.Error("failed to create output file", "path", *outPath, "err", err)
			os.Exit(1)
		}
		defer out.Close()
	} else {
		out = os.Stdout
	}

	w := csv.NewWriter(out)
	header := []string{"blob_hash", "storage", "data_reference"}
	if *includeMeta {
		header = append(header, "meta_reference")
	}
	if err := w.Write(header); err != nil {
		log.Error("failed to write CSV header", "err", err)
		os.Exit(1)
	}
	for _, ref := range refs {
		row := []string{ref.VersionedHash, "ipfs", ref.DataCID}
		if *includeMeta {
			row = append(row, ref.MetaCID)
		}
		if err := w.Write(row); err != nil {
			log.Error("failed to write CSV row", "err", err)
			os.Exit(1)
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		log.Error("CSV write error", "err", err)
		os.Exit(1)
	}

	log.Info("export-blob-refs complete", "rows", len(refs), "from", *fromEpoch, "to", *toEpoch)
}

// summary: human-readable statistics about the indexed data.
func cmdSummary(ctx context.Context, cfg *config.Config, args []string) {
	fs := flag.NewFlagSet("summary", flag.ExitOnError)
	checkIPFS := fs.Bool("check-ipfs", false, "verify epoch node CIDs are present on the IPFS node")
	ipfsStat := fs.Bool("ipfs-stat", false, "query IPFS repo/stat (slow on large repos)")
	showGaps := fs.Bool("gaps", false, "list all missing epoch ranges")
	showEmpty := fs.Bool("empty", false, "list all genuinely empty epochs (no blobs on-chain)")
	topN := fs.Int("top", 0, "show the top N epochs by blob count")
	monthly := fs.Bool("monthly", false, "show a month-by-month breakdown")
	_ = fs.Parse(args)

	if cfg.Storage.PostgresDSN == "" {
		fmt.Fprintln(os.Stderr, "summary: postgres_dsn is required")
		os.Exit(1)
	}

	dbClient, err := db.New(ctx, cfg.Storage.PostgresDSN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "summary: connect to postgres: %v\n", err)
		os.Exit(1)
	}
	defer dbClient.Close()

	const sumWidth = 72
	printSummaryHeader(cfg.Network.Name, sumWidth)

	stats, err := dbClient.GetSummaryStats(ctx, cfg.Network.Name)
	if err != nil {
		fmt.Fprintf(os.Stderr, "summary: query stats: %v\n", err)
		os.Exit(1)
	}

	if stats.EpochCount == 0 {
		fmt.Println("  (no epochs indexed yet)")
		return
	}

	// Fetch gap ranges early; they're cheap and used both in the summary line
	// and in the -gaps detail section.
	var gaps []db.GapRange
	if stats.GapCount > 0 {
		gaps, err = dbClient.GetEpochGapRanges(ctx, cfg.Network.Name)
		if err != nil {
			fmt.Fprintf(os.Stderr, "summary: query gaps: %v\n", err)
			os.Exit(1)
		}
	}

	// ── Default section ───────────────────────────────────────────────────────

	// Epochs line.
	epochsLine := fmt.Sprintf("%s  [%d → %d]",
		formatCount(stats.EpochCount), stats.FirstEpoch, stats.LastEpoch)
	if len(gaps) > 0 {
		coveragePct := float64(stats.EpochCount) / float64(stats.ExpectedEpochCount) * 100
		epochsLine += fmt.Sprintf("  (%d gap range%s · %s epoch%s missing · %.1f%% coverage)",
			len(gaps), pluralS(int64(len(gaps))),
			formatCount(stats.GapCount), pluralS(stats.GapCount),
			coveragePct)
	} else {
		epochsLine += "  (no gaps)"
	}
	printRow("Epochs", epochsLine)

	// Blobs line: total + avg + peak; note empty epochs.
	blobsLine := fmt.Sprintf("%s  (avg %.1f/epoch · peak %s in epoch %d)",
		formatCount(stats.TotalBlobs),
		stats.AvgBlobsPerEpoch,
		formatCount(stats.MaxBlobsPerEpoch),
		stats.MaxBlobsEpoch)
	if stats.EmptyEpochCount > 0 {
		blobsLine += fmt.Sprintf("  · %s empty epoch%s",
			formatCount(stats.EmptyEpochCount), pluralS(stats.EmptyEpochCount))
	}
	printRow("Blobs", blobsLine)

	// Size line: total + avg per epoch.
	avgSize := stats.TotalSizeBytes / stats.EpochCount
	printRow("Data size", fmt.Sprintf("%s  (avg %s/epoch)",
		formatBytes(stats.TotalSizeBytes), formatBytes(avgSize)))

	// Time line.
	if stats.FirstEpochTime != nil && stats.LastEpochTime != nil {
		dur := formatElapsed(*stats.FirstEpochTime, *stats.LastEpochTime)
		printRow("Time", fmt.Sprintf("%s → %s  (%s)",
			stats.FirstEpochTime.UTC().Format("2006-01-02T15:04:05Z"),
			stats.LastEpochTime.UTC().Format("2006-01-02T15:04:05Z"),
			dur))
	}

	// Cursors line.
	cursorLine := fmt.Sprintf("live=%d", stats.LiveCursor)
	if stats.BackfillCursor > 0 {
		cursorLine += fmt.Sprintf("  backfill=%d", stats.BackfillCursor)
	}
	printRow("Cursors", cursorLine)

	// ── IPFS node info + optional epoch check ────────────────────────────────
	if cfg.IPFS.SkipUpload || cfg.IPFS.APIAddr == "" {
		printRow("IPFS", "⚠ not configured (skip_upload=true or no api_addr)")
	} else {
		const checkWorkers = 64
		ipfsClient, err := ipfs.NewClient(cfg.IPFS.APIAddr, cfg.IPFS.Timeout, false, checkWorkers)
		if err != nil {
			printRow("IPFS", fmt.Sprintf("⚠ cannot connect: %v", err))
		} else {
			// Always show node identity.
			nodeInfo, niErr := ipfsClient.GetNodeInfo(ctx)
			if niErr != nil {
				printRow("IPFS node", fmt.Sprintf("⚠ id: %v", niErr))
			} else {
				printRow("IPFS node", fmt.Sprintf("%s  (%s)", nodeInfo.ID, nodeInfo.AgentVersion))
			}
			if *ipfsStat {
				rsCtx, rsCancel := context.WithTimeout(ctx, 5*time.Minute)
				repoStat, rsErr := ipfsClient.GetRepoStat(rsCtx)
				rsCancel()
				if rsErr != nil {
					printRow("IPFS storage", fmt.Sprintf("⚠ repo/stat: %v", rsErr))
				} else {
					usedPct := 0.0
					if repoStat.StorageMax > 0 {
						usedPct = float64(repoStat.RepoSize) / float64(repoStat.StorageMax) * 100
					}
					printRow("IPFS storage", fmt.Sprintf("%s / %s  (%.1f%% used · %s objects)",
						formatBytes(int64(repoStat.RepoSize)),
						formatBytes(int64(repoStat.StorageMax)),
						usedPct,
						formatCount(int64(repoStat.NumObjects))))
				}
			} else {
				printRow("IPFS storage", "use -ipfs-stat to query (slow on large repos)")
			}

			if *checkIPFS {
				present, missingEpochs := checkEpochsInIPFS(ctx, ipfsClient, dbClient, cfg.Network.Name, os.Stderr, checkWorkers)
				total := present + int64(len(missingEpochs))
				pct := 0.0
				if total > 0 {
					pct = float64(present) / float64(total) * 100
				}
				ipfsLine := fmt.Sprintf("%s/%s epoch nodes present  (%.1f%%)",
					formatCount(present), formatCount(total), pct)
				if len(missingEpochs) > 0 {
					const maxShow = 10
					labels := make([]string, 0, min(len(missingEpochs), maxShow))
					for _, e := range missingEpochs {
						if len(labels) >= maxShow {
							break
						}
						labels = append(labels, fmt.Sprintf("%d", e))
					}
					suffix := ""
					if len(missingEpochs) > maxShow {
						suffix = fmt.Sprintf(" … +%d more", len(missingEpochs)-maxShow)
					}
					ipfsLine += "\n                  missing: " + strings.Join(labels, " · ") + suffix
				}
				printRow("IPFS epochs", ipfsLine)
			} else {
				printRow("IPFS epochs", "use -check-ipfs to verify upload status")
			}
		}
	}

	// ── Gap detail ────────────────────────────────────────────────────────────
	if *showGaps {
		if len(gaps) == 0 {
			printSectionHeader("Epoch gaps", "none", sumWidth)
		} else {
			totalMissing := int64(0)
			for _, g := range gaps {
				totalMissing += int64(g.Count())
			}
			printSectionHeader("Epoch gaps",
				fmt.Sprintf("%d range%s · %s epoch%s missing",
					len(gaps), pluralS(int64(len(gaps))),
					formatCount(totalMissing), pluralS(totalMissing)),
				sumWidth)
			for _, g := range gaps {
				if g.Start == g.End {
					fmt.Printf("  %d\n", g.Start)
				} else {
					fmt.Printf("  %d → %d  (%s epoch%s)\n",
						g.Start, g.End,
						formatCount(int64(g.Count())), pluralS(int64(g.Count())))
				}
			}
		}
	}

	// ── Empty epochs detail ───────────────────────────────────────────────────
	if *showEmpty {
		emptyEpochs, err := dbClient.GetEmptyEpochs(ctx, cfg.Network.Name)
		if err != nil {
			fmt.Fprintf(os.Stderr, "\nsummary: query empty epochs: %v\n", err)
			os.Exit(1)
		}
		if len(emptyEpochs) == 0 {
			printSectionHeader("Empty epochs", "none", sumWidth)
		} else {
			printSectionHeader("Empty epochs",
				fmt.Sprintf("%s epoch%s with no blobs on-chain",
					formatCount(int64(len(emptyEpochs))), pluralS(int64(len(emptyEpochs)))),
				sumWidth)
			// Print as compact ranges.
			start := emptyEpochs[0]
			end := emptyEpochs[0]
			flush := func() {
				if start == end {
					fmt.Printf("  %d\n", start)
				} else {
					fmt.Printf("  %d → %d  (%s epoch%s)\n",
						start, end,
						formatCount(int64(end-start+1)), pluralS(int64(end-start+1)))
				}
			}
			for _, e := range emptyEpochs[1:] {
				if e == end+1 {
					end = e
				} else {
					flush()
					start = e
					end = e
				}
			}
			flush()
		}
	}

	// ── Top N epochs ──────────────────────────────────────────────────────────
	if *topN > 0 {
		topRows, err := dbClient.GetTopEpochsByBlobCount(ctx, cfg.Network.Name, *topN)
		if err != nil {
			fmt.Fprintf(os.Stderr, "\nsummary: query top epochs: %v\n", err)
			os.Exit(1)
		}
		printSectionHeader(fmt.Sprintf("Top %d epochs by blob count", *topN), "", sumWidth)
		fmt.Printf("  %-10s  %8s  %10s  %s\n", "EPOCH", "BLOBS", "SIZE", "TIME")
		for _, r := range topRows {
			timeStr := "—"
			if r.EpochTime != nil {
				timeStr = r.EpochTime.UTC().Format("2006-01-02T15:04:05Z")
			}
			fmt.Printf("  %-10d  %8s  %10s  %s\n",
				r.Epoch,
				formatCount(r.BlobCount),
				formatBytes(r.SizeBytes),
				timeStr,
			)
		}
	}

	// ── Monthly breakdown ─────────────────────────────────────────────────────
	if *monthly {
		months, err := dbClient.GetMonthlyStats(ctx, cfg.Network.Name)
		if err != nil {
			fmt.Fprintf(os.Stderr, "\nsummary: query monthly stats: %v\n", err)
			os.Exit(1)
		}
		if len(months) == 0 {
			printSectionHeader("Monthly breakdown", "no epoch_time data available", sumWidth)
		} else {
			printSectionHeader("Monthly breakdown", "", sumWidth)
			fmt.Printf("  %-10s  %8s  %10s  %10s\n", "MONTH", "EPOCHS", "BLOBS", "SIZE")
			for _, m := range months {
				fmt.Printf("  %-10s  %8s  %10s  %10s\n",
					m.Month.UTC().Format("2006-01"),
					formatCount(m.EpochCount),
					formatCount(m.BlobCount),
					formatBytes(m.SizeBytes),
				)
			}
		}
	}
}

// checkEpochsInIPFS reports how many epoch root CIDs are present on the IPFS
// node and which epochs are missing.
//
// repair-epochs: rebuild ipld_epochs rows that were incorrectly saved with
// blob_count=0 by reconstructing the epoch node from cached ipld_blobs rows.
// This fixes epochs that the beacon can no longer serve (beyond retention window)
// but whose blob data is intact in the DB.
func cmdRepairEpochs(ctx context.Context, cfg *config.Config, log *slog.Logger, args []string) {
	fs := flag.NewFlagSet("repair-epochs", flag.ExitOnError)
	dryRun := fs.Bool("dry-run", false, "print corrupted epochs without repairing them")
	noVerify := fs.Bool("no-verify", false, "skip the local-DAG integrity check before saving (trust DB metadata blindly)")
	fs.Parse(args)

	if cfg.Storage.PostgresDSN == "" {
		fmt.Fprintln(os.Stderr, "repair-epochs: postgres_dsn is required")
		os.Exit(1)
	}

	dbClient, err := db.New(ctx, cfg.Storage.PostgresDSN)
	if err != nil {
		log.Error("failed to connect to postgres", "err", err)
		os.Exit(1)
	}
	defer dbClient.Close()

	// Detect any blob_count mismatch (stored ≠ actual row count) in either
	// direction. blob_count is derived from the ipld_blobs rows, so rebuild-from-DB
	// is the right authority for it. (Generalizes the old blob_count=0-only check;
	// use `health-check` for metadata/CID corruption that this cannot detect.)
	mismatches, err := dbClient.GetBlobCountMismatches(ctx, cfg.Network.Name)
	if err != nil {
		log.Error("failed to query blob_count mismatches", "err", err)
		os.Exit(1)
	}

	if len(mismatches) == 0 {
		log.Info("no blob_count mismatches found")
		return
	}

	epochs := make([]uint64, len(mismatches))
	for i, m := range mismatches {
		epochs[i] = m.Epoch
	}

	log.Info("blob_count mismatches found", "count", len(epochs), "first", epochs[0], "last", epochs[len(epochs)-1])

	if *dryRun {
		for _, m := range mismatches {
			fmt.Printf("%d\tstored=%d\tactual=%d\n", m.Epoch, m.Stored, m.Actual)
		}
		return
	}

	gen, err := generator.New(ctx, cfg, log)
	if err != nil {
		log.Error("failed to create generator", "err", err)
		os.Exit(1)
	}

	ok, failed := gen.RepairEpochs(ctx, epochs, !*noVerify, func(epoch uint64, err error) {
		if err != nil {
			log.Error("failed to repair epoch", "epoch", epoch, "err", err)
		} else {
			log.Info("epoch repaired", "epoch", epoch)
		}
	})

	log.Info("repair complete", "repaired", ok, "failed", failed)
	// repair-epochs only fixes blob_count; it trusts the DB's meta/data CIDs.
	// Point the operator at the deeper checks so wrong-CID corruption (which
	// rebuild-from-DB cannot detect) doesn't go unnoticed.
	log.Info("next: run `health-check` to verify CIDs; for epochs it flags, `backfill-ipfs` re-fetches and self-heals")
	if failed > 0 {
		os.Exit(1)
	}
}

// ─── health-check ───────────────────────────────────────────────────────────

// checkStatus is the outcome of a single health check.
type checkStatus int

const (
	statusPass checkStatus = iota
	statusWarn
	statusFail
	statusSkip
)

func (s checkStatus) String() string {
	switch s {
	case statusPass:
		return "PASS"
	case statusWarn:
		return "WARN"
	case statusFail:
		return "FAIL"
	default:
		return "SKIP"
	}
}

// checkResult is one row in the health report.
type checkResult struct {
	name    string
	tier    int
	status  checkStatus
	detail  string   // human summary, e.g. "12 epochs"
	samples []uint64 // example offending epochs (sorted, deduped)
}

// healthReport accumulates check results and tracks remediation hints.
type healthReport struct {
	results []checkResult
	// epochs needing each remedy, for the remediation block.
	repairEpochs   []uint64 // blob_count mismatches → repair-epochs
	backfillEpochs []uint64 // CID corruption → backfill-ipfs
}

func (r *healthReport) add(name string, tier int, status checkStatus, detail string, samples []uint64) {
	sortUint64(samples)
	r.results = append(r.results, checkResult{name, tier, status, detail, samples})
}

// worstStatus returns the most severe status across all checks (skip < pass <
// warn < fail), used for the exit code.
func (r *healthReport) worst() checkStatus {
	worst := statusPass
	for _, c := range r.results {
		if c.status > worst && c.status != statusSkip {
			worst = c.status
		}
	}
	return worst
}

func cmdHealthCheck(ctx context.Context, cfg *config.Config, log *slog.Logger, args []string) {
	fs := flag.NewFlagSet("health-check", flag.ExitOnError)
	maxTier := fs.Int("tier", 1, "highest tier to run (0=SQL invariants, 1=+offline CID recompute)")
	samples := fs.Int("samples", 10, "number of example epochs to show per failing check")
	_ = fs.Parse(args)

	if cfg.Storage.PostgresDSN == "" {
		fmt.Fprintln(os.Stderr, "health-check: postgres_dsn is required")
		os.Exit(1)
	}
	if *maxTier < 0 {
		*maxTier = 0
	}

	dbClient, err := db.New(ctx, cfg.Storage.PostgresDSN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "health-check: connect to postgres: %v\n", err)
		os.Exit(1)
	}
	defer dbClient.Close()

	network := cfg.Network.Name
	const width = 72
	printSummaryHeader(network+" health-check", width)

	rep := &healthReport{}

	runTier0(ctx, dbClient, network, *samples, rep)
	if *maxTier >= 1 {
		runTier1(ctx, cfg, dbClient, network, *samples, rep)
	}

	// ── Render ────────────────────────────────────────────────────────────────
	renderReport(rep, *maxTier, *samples, width)

	// ── Remediation ───────────────────────────────────────────────────────────
	renderRemediation(rep, width)

	switch rep.worst() {
	case statusFail:
		os.Exit(2)
	default:
		// PASS or WARN → success exit.
	}
}

// runTier0 executes the pure-SQL invariant checks.
func runTier0(ctx context.Context, dbClient *db.Client, network string, samples int, rep *healthReport) {
	// a) blob_count mismatch (FAIL) — repairable via repair-epochs.
	if mm, err := dbClient.GetBlobCountMismatches(ctx, network); err != nil {
		rep.add("blob_count match", 0, statusFail, "query error: "+err.Error(), nil)
	} else if len(mm) == 0 {
		rep.add("blob_count match", 0, statusPass, "", nil)
	} else {
		ex := make([]uint64, 0, len(mm))
		for _, m := range mm {
			ex = append(ex, m.Epoch)
			rep.repairEpochs = append(rep.repairEpochs, m.Epoch)
		}
		rep.add("blob_count match", 0, statusFail,
			fmt.Sprintf("%d epoch%s (stored≠actual)", len(mm), pluralS(int64(len(mm)))),
			capSamples(ex, samples))
	}

	// b) size_bytes consistency (WARN — size is approximate).
	if mm, err := dbClient.GetSizeMismatches(ctx, network); err != nil {
		rep.add("size_bytes match", 0, statusFail, "query error: "+err.Error(), nil)
	} else if len(mm) == 0 {
		rep.add("size_bytes match", 0, statusPass, "", nil)
	} else {
		ex := make([]uint64, 0, len(mm))
		for _, m := range mm {
			ex = append(ex, m.Epoch)
		}
		rep.add("size_bytes match", 0, statusWarn,
			fmt.Sprintf("%d epoch%s (approximate)", len(mm), pluralS(int64(len(mm)))),
			capSamples(ex, samples))
	}

	// c) blob/epoch network mismatch (FAIL).
	addAnomalyCheck(ctx, "network match", network, samples, rep, dbClient.GetNetworkMismatches, statusFail, "blobs on wrong network")

	// d) orphan blobs (FAIL).
	addAnomalyCheck(ctx, "orphan blobs", network, samples, rep, dbClient.GetOrphanBlobs, statusFail, "blobs without an epoch row")

	// e) blob_index anomalies: duplicates FAIL, gaps WARN.
	if an, err := dbClient.GetBlobIndexAnomalies(ctx, network); err != nil {
		rep.add("blob_index", 0, statusFail, "query error: "+err.Error(), nil)
	} else {
		var dups, gaps []uint64
		for _, a := range an {
			if a.Rows != a.DistinctIdx {
				dups = append(dups, a.Epoch)
			} else {
				gaps = append(gaps, a.Epoch) // contiguity broken but no dup
			}
		}
		switch {
		case len(dups) > 0:
			rep.add("blob_index", 0, statusFail,
				fmt.Sprintf("%d epoch%s with duplicate index", len(dups), pluralS(int64(len(dups)))),
				capSamples(dups, samples))
		case len(gaps) > 0:
			rep.add("blob_index", 0, statusWarn,
				fmt.Sprintf("%d epoch%s with index gaps", len(gaps), pluralS(int64(len(gaps)))),
				capSamples(gaps, samples))
		default:
			rep.add("blob_index", 0, statusPass, "", nil)
		}
	}

	// f) epoch gap ranges (WARN — missing data, not corrupt). Reuse stats.
	stats, statsErr := dbClient.GetSummaryStats(ctx, network)
	if statsErr != nil {
		rep.add("epoch gaps", 0, statusFail, "query error: "+statsErr.Error(), nil)
		rep.add("cursors", 0, statusFail, "query error: "+statsErr.Error(), nil)
		return
	}
	if stats.GapCount == 0 {
		rep.add("epoch gaps", 0, statusPass, "", nil)
	} else {
		gaps, err := dbClient.GetEpochGapRanges(ctx, network)
		detail := fmt.Sprintf("%s epoch%s missing", formatCount(stats.GapCount), pluralS(stats.GapCount))
		if err == nil {
			detail = fmt.Sprintf("%d range%s · %s", len(gaps), pluralS(int64(len(gaps))), detail)
		}
		rep.add("epoch gaps", 0, statusWarn, detail, nil)
	}

	// g) cursor sanity: live <= max, backfill <= live (WARN on inversion).
	var cursorIssues []string
	if stats.LiveCursor > stats.LastEpoch {
		cursorIssues = append(cursorIssues, fmt.Sprintf("live(%d) > max(%d)", stats.LiveCursor, stats.LastEpoch))
	}
	if stats.BackfillCursor > stats.LiveCursor && stats.BackfillCursor > stats.LastEpoch {
		cursorIssues = append(cursorIssues, fmt.Sprintf("backfill(%d) > live(%d)", stats.BackfillCursor, stats.LiveCursor))
	}
	if len(cursorIssues) == 0 {
		rep.add("cursors", 0, statusPass, "", nil)
	} else {
		rep.add("cursors", 0, statusWarn, strings.Join(cursorIssues, ", "), nil)
	}
}

// addAnomalyCheck runs a per-epoch anomaly query and records a result.
func addAnomalyCheck(ctx context.Context, name, network string, samples int, rep *healthReport,
	query func(context.Context, string) ([]db.EpochAnomaly, error), failStatus checkStatus, what string) {
	an, err := query(ctx, network)
	if err != nil {
		rep.add(name, 0, statusFail, "query error: "+err.Error(), nil)
		return
	}
	if len(an) == 0 {
		rep.add(name, 0, statusPass, "", nil)
		return
	}
	ex := make([]uint64, 0, len(an))
	var total int64
	for _, a := range an {
		ex = append(ex, a.Epoch)
		total += a.Count
	}
	rep.add(name, 0, failStatus,
		fmt.Sprintf("%s %s in %d epoch%s", formatCount(total), what, len(an), pluralS(int64(len(an)))),
		capSamples(ex, samples))
}

// runTier1 recomputes meta_cid (per blob) and the epoch root CID (per epoch)
// from DB rows alone and compares them to the stored values. No network I/O.
func runTier1(ctx context.Context, cfg *config.Config, dbClient *db.Client, network string, samples int, rep *healthReport) {
	epochs, err := dbClient.GetAllEpochs(ctx, network)
	if err != nil {
		rep.add("meta_cid recompute", 1, statusFail, "query error: "+err.Error(), nil)
		rep.add("epoch root recompute", 1, statusFail, "query error: "+err.Error(), nil)
		return
	}

	type epochResult struct {
		epoch    uint64
		metaBad  bool
		rootBad  bool
		blobsBad int64
	}

	total := len(epochs)
	var done atomic.Int64

	results := make([]epochResult, total)
	workers := runtime.NumCPU()
	if workers > 16 {
		workers = 16
	}

	type job struct {
		idx int
		e   db.EpochRecord
	}
	jobCh := make(chan job, workers*2)

	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobCh {
				r := epochResult{epoch: j.e.Epoch}

				records, err := dbClient.GetBlobsByEpoch(ctx, network, j.e.Epoch)
				if err != nil || len(records) == 0 {
					done.Add(1)
					results[j.idx] = r
					continue
				}

				epochInp, blobResults, err := generator.ReconstructFromDB(j.e.Epoch, records)
				if err != nil {
					r.metaBad = true
					done.Add(1)
					results[j.idx] = r
					continue
				}

				for k, rec := range records {
					bs := store.NewMemBlockstore()
					lsys := store.NewLinkSystem(bs)
					metaCID, err := builder.StoreBlobMetadata(ctx, lsys, epochInp.Blobs[k], blobResults[k].DataCID)
					if err != nil {
						r.metaBad = true
						continue
					}
					if metaCID.String() != rec.MetaCID {
						r.metaBad = true
						r.blobsBad++
					}
				}

				bs := store.NewMemBlockstore()
				lsys := store.NewLinkSystem(bs)
				res, err := builder.BuildEpochNode(ctx, lsys, epochInp, blobResults, network, cfg.Generator.HAMTThreshold)
				if err != nil {
					r.rootBad = true
				} else if res.CID.String() != j.e.CID {
					r.rootBad = true
				}

				done.Add(1)
				results[j.idx] = r
			}
		}()
	}

	// Feed jobs and print progress from the main goroutine.
	showProgress := total > 50
	for i, e := range epochs {
		if ctx.Err() != nil {
			break
		}
		if showProgress && i%50 == 0 {
			n := done.Load()
			pct := int(n * 100 / int64(total))
			fmt.Fprintf(os.Stderr, "\r  tier 1: recomputing %d/%d epochs (%d%%)", n, total, pct)
		}
		jobCh <- job{i, e}
	}
	close(jobCh)
	wg.Wait()

	if showProgress {
		fmt.Fprintf(os.Stderr, "\r  tier 1: recomputing %d/%d epochs (100%%)", total, total)
		fmt.Fprint(os.Stderr, "\r\033[K") // clear progress line
	}

	var (
		metaBad []uint64
		rootBad []uint64
		blobBad int64
	)
	for _, r := range results {
		if r.metaBad {
			metaBad = append(metaBad, r.epoch)
			rep.backfillEpochs = append(rep.backfillEpochs, r.epoch)
			blobBad += r.blobsBad
		}
		if r.rootBad {
			rootBad = append(rootBad, r.epoch)
			if !r.metaBad {
				rep.backfillEpochs = append(rep.backfillEpochs, r.epoch)
			}
		}
	}

	if len(metaBad) == 0 {
		rep.add("meta_cid recompute", 1, statusPass, "", nil)
	} else {
		rep.add("meta_cid recompute", 1, statusFail,
			fmt.Sprintf("%d blob%s wrong in %d epoch%s",
				blobBad, pluralS(blobBad), len(metaBad), pluralS(int64(len(metaBad)))),
			capSamples(metaBad, samples))
	}

	// Report root mismatches that have NO underlying meta mismatch separately:
	// those are pure root corruption rather than a downstream effect.
	metaSet := make(map[uint64]struct{}, len(metaBad))
	for _, e := range metaBad {
		metaSet[e] = struct{}{}
	}
	var pureRoot []uint64
	for _, e := range rootBad {
		if _, ok := metaSet[e]; !ok {
			pureRoot = append(pureRoot, e)
		}
	}
	switch {
	case len(rootBad) == 0:
		rep.add("epoch root recompute", 1, statusPass, "", nil)
	case len(pureRoot) > 0:
		rep.add("epoch root recompute", 1, statusFail,
			fmt.Sprintf("%d epoch%s (%d not explained by meta_cid)",
				len(rootBad), pluralS(int64(len(rootBad))), len(pureRoot)),
			capSamples(pureRoot, samples))
	default:
		rep.add("epoch root recompute", 1, statusFail,
			fmt.Sprintf("%d epoch%s (all correlate with meta_cid)", len(rootBad), pluralS(int64(len(rootBad)))),
			capSamples(rootBad, samples))
	}
}

// renderReport prints the per-tier check table.
func renderReport(rep *healthReport, maxTier, samples, width int) {
	for tier := 0; tier <= 1; tier++ {
		var rows []checkResult
		for _, c := range rep.results {
			if c.tier == tier {
				rows = append(rows, c)
			}
		}
		title := fmt.Sprintf("Tier %d", tier)
		if tier == 0 {
			title += "  SQL invariants"
		} else {
			title += "  offline CID recompute"
		}
		if tier > maxTier {
			printSectionHeader(title, "skipped — raise -tier", width)
			continue
		}
		printSectionHeader(title, "", width)
		for _, c := range rows {
			line := fmt.Sprintf("%-6s %s", c.status, c.detail)
			if len(c.samples) > 0 {
				line += "  e.g. " + formatEpochSamples(c.samples, samples)
			}
			printRow(c.name, strings.TrimRight(line, " "))
		}
	}
}

// renderRemediation prints suggested follow-up commands.
func renderRemediation(rep *healthReport, width int) {
	repair := dedupUint64(rep.repairEpochs)
	backfill := dedupUint64(rep.backfillEpochs)
	if len(repair) == 0 && len(backfill) == 0 {
		return
	}
	printSectionHeader("Suggested remediation", "", width)
	if len(repair) > 0 {
		fmt.Printf("  blobscan-ipld repair-epochs        # fix %d blob_count mismatch%s\n",
			len(repair), map[bool]string{true: "es", false: ""}[len(repair) != 1])
	}
	for _, rng := range collapseRanges(backfill) {
		if rng[0] == rng[1] {
			fmt.Printf("  blobscan-ipld backfill-ipfs -from %d -to %d   # re-fetch & self-heal CIDs\n", rng[0], rng[1])
		} else {
			fmt.Printf("  blobscan-ipld backfill-ipfs -from %d -to %d   # re-fetch & self-heal %d epochs\n",
				rng[0], rng[1], rng[1]-rng[0]+1)
		}
	}
}

// ── health-check helpers ────────────────────────────────────────────────────

// capSamples returns at most n epochs (already the caller's offending list).
func capSamples(epochs []uint64, n int) []uint64 {
	if len(epochs) <= n {
		return epochs
	}
	return epochs[:n]
}

// formatEpochSamples renders sample epochs as "a · b · c … +N".
func formatEpochSamples(epochs []uint64, max int) string {
	shown := epochs
	extra := 0
	if len(epochs) > max {
		shown = epochs[:max]
		extra = len(epochs) - max
	}
	parts := make([]string, len(shown))
	for i, e := range shown {
		parts[i] = fmt.Sprintf("%d", e)
	}
	s := strings.Join(parts, " · ")
	if extra > 0 {
		s += fmt.Sprintf(" … +%d", extra)
	}
	return s
}

func dedupUint64(in []uint64) []uint64 {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[uint64]struct{}, len(in))
	out := make([]uint64, 0, len(in))
	for _, v := range in {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	sortUint64(out)
	return out
}

// collapseRanges turns a sorted, deduped epoch list into contiguous [start,end]
// ranges.
func collapseRanges(epochs []uint64) [][2]uint64 {
	epochs = dedupUint64(epochs)
	if len(epochs) == 0 {
		return nil
	}
	var out [][2]uint64
	start, end := epochs[0], epochs[0]
	for _, e := range epochs[1:] {
		if e == end+1 {
			end = e
		} else {
			out = append(out, [2]uint64{start, end})
			start, end = e, e
		}
	}
	out = append(out, [2]uint64{start, end})
	return out
}

// Fast path: fetch the node's recursive pin set in a single pin/ls request and
// resolve every epoch with an in-memory lookup — O(1) HTTP regardless of epoch
// count. This works because epoch roots are pinned on upload (see
// IPFSConfig.PinOnAdd, on by default). If pin/ls is unavailable or pinning is
// disabled, fall back to one block/stat per epoch across a worker pool.
func checkEpochsInIPFS(ctx context.Context, ipfsClient *ipfs.Client, dbClient *db.Client, network string, progress io.Writer, workers int) (int64, []uint64) {
	records, err := dbClient.GetAllEpochs(ctx, network)
	if err != nil || len(records) == 0 {
		return 0, nil
	}

	if pins, err := ipfsClient.ListRecursivePins(ctx); err == nil {
		return checkEpochsAgainstPins(records, pins, progress)
	} else if progress != nil {
		fmt.Fprintf(progress, "  pin/ls unavailable (%v); falling back to per-epoch block/stat\n", err)
	}
	return checkEpochsByBlockStat(ctx, ipfsClient, records, progress, workers)
}

// checkEpochsAgainstPins resolves each epoch by membership in an already-fetched
// recursive pin set, comparing canonical CID strings.
func checkEpochsAgainstPins(records []db.EpochRecord, pins map[string]struct{}, progress io.Writer) (int64, []uint64) {
	var present int64
	var missing []uint64
	for _, rec := range records {
		ok := false
		if c, err := cid.Decode(rec.CID); err == nil {
			_, ok = pins[c.String()]
		}
		if ok {
			present++
		} else {
			missing = append(missing, rec.Epoch)
		}
	}
	if progress != nil {
		fmt.Fprintf(progress, "  checking IPFS: %d/%d epochs (pin set)\n", len(records), len(records))
	}
	sortUint64(missing)
	return present, missing
}

// checkEpochsByBlockStat is the fallback path: one block/stat per epoch fanned
// out across a worker pool. Used when the recursive pin set is unavailable.
func checkEpochsByBlockStat(ctx context.Context, ipfsClient *ipfs.Client, records []db.EpochRecord, progress io.Writer, workers int) (int64, []uint64) {
	total := len(records)

	type checkResult struct {
		epoch   uint64
		present bool
	}

	jobs := make(chan db.EpochRecord, total)
	results := make(chan checkResult, total)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for rec := range jobs {
				c, err := cid.Decode(rec.CID)
				ok := false
				if err == nil {
					ok, _ = ipfsClient.HasBlock(ctx, c)
				}
				results <- checkResult{epoch: rec.Epoch, present: ok}
			}
		}()
	}
	for _, r := range records {
		jobs <- r
	}
	close(jobs)
	go func() { wg.Wait(); close(results) }()

	var present int64
	var missing []uint64
	done := 0
	for r := range results {
		done++
		if r.present {
			present++
		} else {
			missing = append(missing, r.epoch)
		}
		if progress != nil {
			fmt.Fprintf(progress, "\r  checking IPFS: %d/%d epochs (%.0f%%)   ",
				done, total, float64(done)/float64(total)*100)
		}
	}
	if progress != nil {
		fmt.Fprintln(progress)
	}
	sortUint64(missing)
	return present, missing
}

// ── Summary formatting helpers ────────────────────────────────────────────────

func printSummaryHeader(network string, width int) {
	title := "── blobscan-ipld summary ── " + network + " "
	pad := width - len(title)
	if pad < 2 {
		pad = 2
	}
	fmt.Println(title + strings.Repeat("─", pad))
}

func printSectionHeader(title, detail string, width int) {
	line := "── " + title + " "
	if detail != "" {
		line += "(" + detail + ") "
	}
	pad := width - len(line)
	if pad < 2 {
		pad = 2
	}
	fmt.Println("\n" + line + strings.Repeat("─", pad))
}

func printRow(label, value string) {
	fmt.Printf("  %-16s %s\n", label, value)
}

// formatBytes formats a byte count as a human-readable string (GiB/MiB/KiB/B).
func formatBytes(n int64) string {
	const (
		gib = 1 << 30
		mib = 1 << 20
		kib = 1 << 10
	)
	switch {
	case n >= gib:
		return fmt.Sprintf("%.2f GiB", float64(n)/gib)
	case n >= mib:
		return fmt.Sprintf("%.1f MiB", float64(n)/mib)
	case n >= kib:
		return fmt.Sprintf("%.1f KiB", float64(n)/kib)
	default:
		return fmt.Sprintf("%d B", n)
	}
}

// formatCount formats an integer with comma thousands separators.
func formatCount(n int64) string {
	if n < 0 {
		return "-" + formatCount(-n)
	}
	s := fmt.Sprintf("%d", n)
	out := make([]byte, 0, len(s)+(len(s)-1)/3)
	for i, ch := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, byte(ch))
	}
	return string(out)
}

// formatElapsed returns a human-readable duration between two times.
func formatElapsed(a, b time.Time) string {
	d := b.Sub(a)
	if d < 0 {
		d = -d
	}
	days := int(d.Hours() / 24)
	switch {
	case days >= 2:
		return fmt.Sprintf("%d days", days)
	case days == 1:
		return "1 day"
	default:
		h := int(d.Hours())
		if h >= 1 {
			return fmt.Sprintf("%dh", h)
		}
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
}

// pluralS returns "s" when n != 1, "" otherwise.
func pluralS(n int64) string {
	if n == 1 {
		return ""
	}
	return "s"
}

// sortUint64 sorts a uint64 slice in ascending order.
func sortUint64(a []uint64) {
	for i := 1; i < len(a); i++ {
		for j := i; j > 0 && a[j] < a[j-1]; j-- {
			a[j], a[j-1] = a[j-1], a[j]
		}
	}
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

// teeHandler fans a slog record out to two handlers. Used to send Error-level
// logs to Sentry while still writing them to the original handler.
type teeHandler struct {
	primary   slog.Handler
	secondary slog.Handler
}

func newTeeHandler(primary, secondary slog.Handler) *teeHandler {
	return &teeHandler{primary: primary, secondary: secondary}
}

func (h *teeHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.primary.Enabled(ctx, level) || h.secondary.Enabled(ctx, level)
}

func (h *teeHandler) Handle(ctx context.Context, r slog.Record) error {
	_ = h.secondary.Handle(ctx, r)
	return h.primary.Handle(ctx, r)
}

func (h *teeHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return newTeeHandler(h.primary.WithAttrs(attrs), h.secondary.WithAttrs(attrs))
}

func (h *teeHandler) WithGroup(name string) slog.Handler {
	return newTeeHandler(h.primary.WithGroup(name), h.secondary.WithGroup(name))
}

func newIPFSClientFromConfig(cfg *config.Config) (*ipfs.Client, error) {
	return ipfs.NewClient(cfg.IPFS.APIAddr, cfg.IPFS.Timeout, cfg.IPFS.PinOnAdd, cfg.IPFS.UploadWorkers)
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: lvl,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				a.Value = slog.StringValue(a.Value.Time().Format("2006-01-02T15:04:05Z07:00"))
			}
			return a
		},
	})
	return slog.New(handler)
}
