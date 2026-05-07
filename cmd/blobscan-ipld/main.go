// Command blobscan-ipld is the main entry point for the Blobscan IPLD DAG
// generator. It exposes several subcommands for different operation modes.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

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

Global flags (before subcommand):
  -config <path>      Path to YAML config file (default: config.yaml)
  -log-level <level>  Log level: debug, info, warn, error (default: info)

Examples:
  blobscan-ipld -config mainnet.yaml run
  blobscan-ipld -config mainnet.yaml serve
  blobscan-ipld -config mainnet.yaml -n 300000 epoch
  blobscan-ipld -config mainnet.yaml -n 300000 finalize-epoch
  blobscan-ipld -config mainnet.yaml -n 300000 -out /tmp/300000.car export-car
  blobscan-ipld -config mainnet.yaml -from 300000 -to 300099 -out /tmp/range.car export-car-range
`

func main() {
	// Global flags parsed before the subcommand.
	globalFlags := flag.NewFlagSet("blobscan-ipld", flag.ExitOnError)
	configPath := globalFlags.String("config", "config.yaml", "path to YAML config file")
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

	cfg, err := config.Load(*configPath)
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

// ─── Helpers ──────────────────────────────────────────────────────────────────

func newIPFSClientFromConfig(cfg *config.Config) (*ipfs.Client, error) {
	return ipfs.NewClient(cfg.IPFS.APIAddr, cfg.IPFS.Timeout)
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
