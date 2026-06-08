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
  export-blob-refs Export blob CID references as CSV for import into blobscan DB
  summary          Show indexed-data statistics (use -help for detail flags)

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
  blobscan-ipld export-blob-refs -out /tmp/refs.csv
  blobscan-ipld export-blob-refs -from 300000 -to 300099
  blobscan-ipld export-blob-refs -meta -out /tmp/refs.csv
  blobscan-ipld summary
  blobscan-ipld summary -gaps -top 10 -monthly -check-ipfs
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
	case "export-blob-refs":
		cmdExportBlobRefs(ctx, cfg, log, subArgs)
	case "summary":
		cmdSummary(ctx, cfg, subArgs)
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
	blobs, err := dbClient.GetBlobsByEpoch(ctx, *epochNum)
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
		blobs, err := dbClient.GetBlobsByEpoch(ctx, e.record.Epoch)
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

	refs, err := dbClient.GetBlobRefs(ctx, *fromEpoch, *toEpoch)
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
	showGaps := fs.Bool("gaps", false, "list all missing epoch ranges")
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

	// Blobs line: total + avg + peak.
	blobsLine := fmt.Sprintf("%s  (avg %.1f/epoch · peak %s in epoch %d)",
		formatCount(stats.TotalBlobs),
		stats.AvgBlobsPerEpoch,
		formatCount(stats.MaxBlobsPerEpoch),
		stats.MaxBlobsEpoch)
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

	// ── IPFS check ────────────────────────────────────────────────────────────
	if *checkIPFS {
		if cfg.IPFS.SkipUpload || cfg.IPFS.APIAddr == "" {
			printRow("IPFS", "⚠ not configured (skip_upload=true or no api_addr)")
		} else {
			const checkWorkers = 64
			ipfsClient, err := ipfs.NewClient(cfg.IPFS.APIAddr, cfg.IPFS.Timeout, false, checkWorkers)
			if err != nil {
				printRow("IPFS", fmt.Sprintf("⚠ cannot connect: %v", err))
			} else {
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
				printRow("IPFS", ipfsLine)
			}
		}
	} else {
		printRow("IPFS", "use -check-ipfs to verify upload status")
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

// checkEpochsInIPFS checks all epoch node CIDs against the IPFS node using a
// worker pool sized by the workers parameter. Returns the count of present
// epochs and a sorted list of missing epoch numbers.
func checkEpochsInIPFS(ctx context.Context, ipfsClient *ipfs.Client, dbClient *db.Client, network string, progress io.Writer, workers int) (int64, []uint64) {
	records, err := dbClient.GetAllEpochs(ctx, network)
	if err != nil || len(records) == 0 {
		return 0, nil
	}
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
