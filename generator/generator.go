// Package generator is the main orchestrator that ties together all modules:
// beacon fetching (optional) → blob processing → epoch DAG building →
// IPFS upload → NetworkRoot rebuild → PostgreSQL persistence (optional).
// CAR export is a separate CLI command, not part of the live pipeline.
package generator

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"

	"github.com/blobscan/blobscan-ipld/api"
	"github.com/blobscan/blobscan-ipld/beacon"
	"github.com/blobscan/blobscan-ipld/blobscan"
	"github.com/blobscan/blobscan-ipld/builder"
	"github.com/blobscan/blobscan-ipld/config"
	"github.com/blobscan/blobscan-ipld/db"
	"github.com/blobscan/blobscan-ipld/ipfs"
	"github.com/blobscan/blobscan-ipld/state"
	"github.com/blobscan/blobscan-ipld/store"
	"github.com/blobscan/blobscan-ipld/types"
)

// Generator is the top-level DAG generation orchestrator.
type Generator struct {
	cfg            *config.Config
	beacon         *beacon.Client // nil when beacon_rpc is not configured
	ipfs           *ipfs.Client
	db             *db.Client
	state          state.Backend
	notifier       *blobscan.Notifier // nil when blobscan.api_url is not configured
	log            *slog.Logger
	genesisTime    time.Time // zero if not yet fetched or beacon unavailable
	slotsPerEpoch  uint64
	secondsPerSlot uint64
}

// epochProgress carries batch-level progress state for ETA calculation.
type epochProgress struct {
	idx        int       // 0-based index of this epoch in the current batch
	total      int       // total epochs in this batch
	batchStart time.Time // when the batch started
	src        string    // "live" or "backfill" — used as a log field
}

// New creates a new Generator from the given configuration.
func New(ctx context.Context, cfg *config.Config, log *slog.Logger) (*Generator, error) {
	log.Info("╔═══════════════════════════════════════════════════════════╗")
	log.Info("║                  blobscan-ipld engine                     ║")
	log.Info("║         Building IPLD DAGs from Ethereum blobs            ║")
	log.Info("╚═══════════════════════════════════════════════════════════╝")

	var beaconClient *beacon.Client
	if cfg.Network.BeaconRPC != "" {
		beaconClient = beacon.NewClient(
			cfg.Network.BeaconRPC,
			cfg.Network.BeaconTimeout,
			cfg.Generator.BeaconWorkers,
			cfg.Network.BeaconRateLimit,
			cfg.Network.BeaconRateBurst,
			cfg.Network.Beacon429Backoff,
			cfg.Network.SlotsPerEpoch,
			cfg.Network.SecondsPerSlot,
		)
	}

	var ipfsClient *ipfs.Client
	if !cfg.IPFS.SkipUpload {
		var err error
		ipfsClient, err = ipfs.NewClient(cfg.IPFS.APIAddr, cfg.IPFS.Timeout, cfg.IPFS.PinOnAdd, cfg.IPFS.UploadWorkers)
		if err != nil {
			return nil, fmt.Errorf("generator: create ipfs client: %w", err)
		}
		ipfsClient.SetLogger(log)
		log.Info("✓ IPFS upload enabled", "api_addr", cfg.IPFS.APIAddr, "pin_on_add", cfg.IPFS.PinOnAdd)
	} else {
		log.Info("⊘ IPFS upload disabled (skip_upload=true); CIDs will be computed but not uploaded")
	}

	var dbClient *db.Client
	if cfg.Storage.PostgresDSN != "" {
		var err error
		dbClient, err = db.New(ctx, cfg.Storage.PostgresDSN)
		if err != nil {
			return nil, fmt.Errorf("generator: connect to postgres: %w", err)
		}
	} else {
		log.Warn("postgres_dsn not set — DB persistence disabled; export-car, finalize-epoch, and NetworkRoot rebuild will not work")
	}

	var stateBackend state.Backend
	if dbClient != nil {
		stateBackend = db.NewDBStateBackend(dbClient, cfg.Network.Name)
		log.Info("✓ Using PostgreSQL state backend")
	} else {
		// Fall back to the JSON file-based state manager.
		stateMgr, err := state.NewManager(cfg.Storage.DataDir, cfg.Network.Name)
		if err != nil {
			return nil, fmt.Errorf("generator: load state: %w", err)
		}
		stateBackend = stateMgr
		log.Info("✓ Using file-based state backend", "path", cfg.Storage.DataDir)
	}

	notifier := blobscan.NewNotifier(cfg.Blobscan.APIURL, cfg.Blobscan.APIKey, log)
	if notifier != nil {
		log.Info("✓ Blobscan notifier enabled", "api_url", cfg.Blobscan.APIURL)
	}

	g := &Generator{
		cfg:            cfg,
		beacon:         beaconClient,
		ipfs:           ipfsClient,
		db:             dbClient,
		state:          stateBackend,
		notifier:       notifier,
		log:            log,
		slotsPerEpoch:  cfg.Network.SlotsPerEpoch,
		secondsPerSlot: cfg.Network.SecondsPerSlot,
	}

	if beaconClient != nil {
		remoteNetwork, err := beaconClient.GetNetworkName(ctx)
		if err != nil {
			log.Warn("⚠ Could not verify beacon network name", "err", err)
		} else if remoteNetwork != cfg.Network.Name {
			return nil, fmt.Errorf("generator: network mismatch: config says %q but beacon node reports %q", cfg.Network.Name, remoteNetwork)
		} else {
			if gt, err := beaconClient.GetGenesisTime(ctx); err != nil {
				log.Warn("⚠ Could not fetch genesis time; block timestamps will be omitted", "err", err)
				log.Info("✓ Beacon network verified [" + remoteNetwork + "]")
			} else {
				g.genesisTime = gt
				log.Info("✓ Beacon network verified ["+remoteNetwork+"]", "genesis_time", gt.Format(time.RFC3339))
			}
		}
	}

	return g, nil
}

// Close releases resources held by the generator (DB pool, etc.).
func (g *Generator) Close() {
	if g.db != nil {
		g.db.Close()
	}
}

// Run starts the epoch processing pipeline. It blocks until ctx is cancelled.
// Requires beacon_rpc to be configured.
//
// When there is a historical gap between start_epoch and the current finalized
// tip, two goroutines run concurrently:
//   - live: polls for newly finalized epochs and processes them going forward.
//   - backfill: crawls historical epochs from start_epoch up to the live anchor.
//
// If no DB is available, backfill is skipped (we cannot track two cursors
// independently without a persistent store) and the old sequential loop is used.
func (g *Generator) Run(ctx context.Context) error {
	if g.beacon == nil {
		return fmt.Errorf("generator: beacon_rpc is required for the pull loop; use the push API instead")
	}

	// Without a DB we cannot distinguish the two cursors efficiently: fall back
	// to the simple sequential loop.
	if g.db == nil {
		return g.runSequential(ctx)
	}

	checkpoints, err := g.beacon.GetFinalityCheckpoints(ctx, "head")
	if err != nil {
		return fmt.Errorf("generator: get initial finality checkpoints: %w", err)
	}
	currentFinalized := checkpoints.FinalizedEpoch

	liveCursor, err := g.state.GetLastProcessedEpoch(ctx)
	if err != nil {
		return fmt.Errorf("generator: get live cursor: %w", err)
	}
	backfillCursor, err := g.state.GetBackfillCursor(ctx)
	if err != nil {
		return fmt.Errorf("generator: get backfill cursor: %w", err)
	}

	backfillStart := g.cfg.Generator.StartEpoch
	if backfillCursor > 0 {
		backfillStart = backfillCursor + 1
	}

	// Always anchor the live goroutine at the current tip on startup.
	// Any epochs between the old live cursor and the new anchor are handed to
	// the backfill goroutine; ones already in the DB are skipped instantly via
	// EpochExists. This ensures correct behaviour for:
	//   1. Fresh deployments (liveCursor == 0)
	//   2. Migrations from the old sequential loop (liveCursor == MAX(epoch) fallback)
	//   3. Resumed parallel runs where cursors are already set
	if backfillStart < currentFinalized {
		liveCursor = currentFinalized - 1
		if err := g.state.SetLastProcessedEpoch(ctx, liveCursor); err != nil {
			return fmt.Errorf("generator: set live cursor: %w", err)
		}
		g.log.Info("┌─ Parallel processing enabled", "live_cursor", currentFinalized, "backfill_from", backfillStart)
	}

	// Launch backfill goroutine for all epochs before the live anchor.
	if backfillStart < liveCursor {
		go func() {
			if err := g.runBackfill(ctx, backfillStart, liveCursor-1); err != nil && !errors.Is(err, context.Canceled) {
				g.log.Error("backfill goroutine error", "err", err)
			}
		}()
	}

	return g.runLive(ctx)
}

// runSequential is the original single-goroutine loop, used when no DB is
// available and two-cursor tracking is not possible.
func (g *Generator) runSequential(ctx context.Context) error {
	g.log.Info("generator starting (sequential mode)",
		"network", g.cfg.Network.Name,
		"poll_interval", g.cfg.Generator.PollInterval,
	)

	ticker := time.NewTicker(g.cfg.Generator.PollInterval)
	defer ticker.Stop()

	if err := g.processLiveTick(ctx); err != nil {
		if errors.Is(err, beacon.ErrInsufficientCustody) {
			g.log.Error("aborting: beacon node cannot serve blob sidecars", "err", err)
			return err
		}
		g.log.Error("initial epoch processing failed", "err", err)
	}

	for {
		select {
		case <-ctx.Done():
			g.log.Info("generator shutting down")
			return ctx.Err()
		case <-ticker.C:
			if err := g.processLiveTick(ctx); err != nil {
				if errors.Is(err, beacon.ErrInsufficientCustody) {
					g.log.Error("aborting: beacon node cannot serve blob sidecars", "err", err)
					return err
				}
				g.log.Error("epoch processing failed", "err", err)
			}
		}
	}
}

// runLive polls for newly finalized epochs and processes them. It blocks until
// ctx is cancelled.
func (g *Generator) runLive(ctx context.Context) error {
	g.log.Info("▶ Live processing started [" + g.cfg.Network.Name + "] — polling every " + g.cfg.Generator.PollInterval.String())

	ticker := time.NewTicker(g.cfg.Generator.PollInterval)
	defer ticker.Stop()

	if err := g.processLiveTick(ctx); err != nil {
		if errors.Is(err, beacon.ErrInsufficientCustody) {
			g.log.Error("aborting: beacon node cannot serve blob sidecars", "err", err)
			return err
		}
		g.log.Error("initial live tick failed", "src", "live", "err", err)
	}

	for {
		select {
		case <-ctx.Done():
			g.log.Info("generator shutting down")
			return ctx.Err()
		case <-ticker.C:
			if err := g.processLiveTick(ctx); err != nil {
				if errors.Is(err, beacon.ErrInsufficientCustody) {
					g.log.Error("aborting: beacon node cannot serve blob sidecars", "err", err)
					return err
				}
				g.log.Error("live tick failed", "src", "live", "err", err)
			}
		}
	}
}

// processLiveTick fetches the current finalized epoch and processes any epochs
// newer than the live cursor.
func (g *Generator) processLiveTick(ctx context.Context) error {
	checkpoints, err := g.beacon.GetFinalityCheckpoints(ctx, "head")
	if err != nil {
		return fmt.Errorf("generator: get finality checkpoints: %w", err)
	}

	liveCursor, err := g.state.GetLastProcessedEpoch(ctx)
	if err != nil {
		return fmt.Errorf("generator: get live cursor: %w", err)
	}

	startEpoch := g.cfg.Generator.StartEpoch
	if liveCursor > 0 {
		startEpoch = liveCursor + 1
	}

	finalizedEpoch := checkpoints.FinalizedEpoch
	if finalizedEpoch < startEpoch {
		g.log.Debug("no new finalized epochs", "finalized", finalizedEpoch, "live_cursor", liveCursor)
		return nil
	}

	total := int(finalizedEpoch-startEpoch) + 1
	g.log.Info(fmt.Sprintf("▲ %d epoch%s finalized [%d .. %d]", total, pluralize(total), startEpoch, finalizedEpoch))

	batchStart := time.Now()
	for epoch := startEpoch; epoch <= finalizedEpoch; epoch++ {
		p := &epochProgress{
			idx:        int(epoch - startEpoch),
			total:      total,
			batchStart: batchStart,
			src:        "live",
		}
		if err := g.processEpoch(ctx, epoch, p); err != nil {
			return fmt.Errorf("generator: process epoch %d: %w", epoch, err)
		}
		if err := g.state.SetLastProcessedEpoch(ctx, epoch); err != nil {
			return fmt.Errorf("generator: set live cursor %d: %w", epoch, err)
		}
	}

	g.log.Debug("rebuilding network root", "through_epoch", finalizedEpoch)
	if err := g.rebuildNetworkRoot(ctx); err != nil {
		g.log.Warn("network root rebuild failed (non-fatal)", "epoch", finalizedEpoch, "err", err)
	}

	return nil
}

// runBackfill processes epochs from startEpoch to targetEpoch (inclusive),
// skipping any epoch already saved in the DB. It updates the backfill cursor
// after each epoch and rebuilds the NetworkRoot once at completion.
//
// Internally a two-stage pipeline overlaps the beacon fetch + CID build of
// epoch N+1 with the IPFS upload + DB persist of epoch N. Strict ordering is
// preserved by a single buffered channel and a single-threaded consumer, so
// SetBackfillCursor still advances monotonically and crash recovery semantics
// match the pre-pipeline behaviour. On any error, the pipeline is torn down,
// we sleep one PollInterval, and the outer retry loop resumes from the latest
// cursor.
func (g *Generator) runBackfill(ctx context.Context, startEpoch, targetEpoch uint64) error {
	total := int(targetEpoch-startEpoch) + 1
	g.log.Info(fmt.Sprintf("⟲ Backfill: %d epochs [%d → %d]", total, startEpoch, targetEpoch),
		"epoch_workers", g.cfg.Generator.BackfillEpochWorkers)

	batchStart := time.Now()
	next := startEpoch
	for next <= targetEpoch {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		resumed, err := g.runBackfillPipeline(ctx, next, targetEpoch, startEpoch, total, batchStart)
		if err == nil {
			next = resumed
			break
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		g.log.Error("backfill pipeline failed, retrying", "from", next, "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(g.cfg.Generator.PollInterval):
		}
		next = resumed // resumed reflects the last successfully persisted epoch + 1
	}

	g.log.Info("backfill complete", "from", startEpoch, "to", targetEpoch)
	if err := g.rebuildNetworkRoot(ctx); err != nil {
		g.log.Warn("backfill: final network root rebuild failed (non-fatal)", "err", err)
	}
	return nil
}

// runBackfillPipeline runs one attempt of the producer/consumer pipeline. It
// returns (lastEpochProcessed+1, nil) on full completion or (resumePoint, err)
// on failure so the caller can retry from where the consumer last advanced
// the cursor.
//
// Multiple producer goroutines (BackfillEpochWorkers) build epochs in parallel.
// A dispatcher assigns epochs in order and results are collected into a reorder
// buffer so the single-threaded consumer still persists and advances the cursor
// monotonically. The beacon rate limiter is shared across all workers, so the
// configured BEACON_RATE_LIMIT is never exceeded regardless of worker count.
func (g *Generator) runBackfillPipeline(
	ctx context.Context,
	from, targetEpoch, startEpoch uint64,
	total int,
	batchStart time.Time,
) (uint64, error) {
	type job struct {
		epoch uint64
		be    builtEpoch
		err   error
	}

	pctx, cancel := context.WithCancel(ctx)
	defer cancel()

	workers := g.cfg.Generator.BackfillEpochWorkers
	if workers <= 0 {
		workers = 1
	}

	// The ordered channel delivers results to the consumer in epoch order.
	// Buffer enough to keep all workers busy plus one for the dispatcher.
	pending := make(chan job, workers+1)

	// Track the last epoch the consumer successfully persisted (or skipped via
	// EpochExists), so on error we can resume from the right place.
	var nextResume uint64 = from

	// ── Dispatcher + worker pool ──────────────────────────────────────────
	//
	// The dispatcher sends epoch numbers to a work channel; workers pick them
	// up, call buildEpoch, and send results to per-epoch slots in a reorder
	// map. A separate collector goroutine drains the reorder map in order and
	// pushes jobs onto the `pending` channel for the consumer.
	//
	// This ensures:
	//   1. Up to `workers` buildEpoch calls run concurrently.
	//   2. The consumer sees results in strict epoch order.
	//   3. On error, the pipeline tears down quickly via context cancellation.

	type buildResult struct {
		epoch uint64
		be    builtEpoch
		err   error
	}

	workCh := make(chan uint64, workers)
	resultCh := make(chan buildResult, workers)

	// Workers: each picks epochs from workCh and builds them.
	var workerWg sync.WaitGroup
	for i := 0; i < workers; i++ {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			for epoch := range workCh {
				if pctx.Err() != nil {
					resultCh <- buildResult{epoch: epoch, err: pctx.Err()}
					return
				}

				// Skip epochs already saved in the DB for this network.
				if g.db != nil {
					exists, err := g.db.EpochExists(pctx, g.cfg.Network.Name, epoch)
					if err != nil {
						resultCh <- buildResult{epoch: epoch, err: fmt.Errorf("backfill: check epoch %d: %w", epoch, err)}
						return
					}
					if exists {
						resultCh <- buildResult{epoch: epoch, be: builtEpoch{epoch: epoch, skip: true}}
						continue
					}
				}

				p := &epochProgress{
					idx:        int(epoch - startEpoch),
					total:      total,
					batchStart: batchStart,
					src:        "backfill",
				}
				be, err := g.buildEpoch(pctx, epoch, p)
				resultCh <- buildResult{epoch: epoch, be: be, err: err}
				if err != nil {
					return
				}
			}
		}()
	}

	// Dispatcher: sends epochs to workers, then waits for all workers to finish.
	dispatchDone := make(chan struct{})
	go func() {
		defer close(dispatchDone)
		for epoch := from; epoch <= targetEpoch; epoch++ {
			select {
			case workCh <- epoch:
			case <-pctx.Done():
				close(workCh)
				return
			}
		}
		close(workCh)
		workerWg.Wait()
		close(resultCh)
	}()

	// Collector: reorders results and pushes them onto `pending` in epoch order.
	producerDone := make(chan error, 1)
	go func() {
		defer close(pending)
		reorder := make(map[uint64]buildResult)
		nextExpected := from

		for r := range resultCh {
			reorder[r.epoch] = r

			// Flush contiguous results in order.
			for {
				res, ok := reorder[nextExpected]
				if !ok {
					break
				}
				delete(reorder, nextExpected)

				j := job{epoch: res.epoch, be: res.be, err: res.err}
				select {
				case pending <- j:
				case <-pctx.Done():
					producerDone <- pctx.Err()
					return
				}
				if res.err != nil {
					producerDone <- nil
					return
				}
				nextExpected++
			}
		}

		// Flush any remaining reordered results (all workers finished).
		for nextExpected <= targetEpoch {
			res, ok := reorder[nextExpected]
			if !ok {
				break
			}
			delete(reorder, nextExpected)

			j := job{epoch: res.epoch, be: res.be, err: res.err}
			select {
			case pending <- j:
			case <-pctx.Done():
				producerDone <- pctx.Err()
				return
			}
			if res.err != nil {
				producerDone <- nil
				return
			}
			nextExpected++
		}

		producerDone <- nil
	}()

	// ── Consumer: persist epochs in order, advance the cursor ─────────────
	for j := range pending {
		if j.err != nil {
			cancel()
			for range pending {
			}
			<-dispatchDone
			<-producerDone
			return nextResume, j.err
		}
		if !j.be.skip {
			if err := g.persistEpoch(pctx, j.be); err != nil {
				cancel()
				for range pending {
				}
				<-dispatchDone
				<-producerDone
				return nextResume, fmt.Errorf("backfill: persist epoch %d: %w", j.epoch, err)
			}
		}
		if err := g.state.SetBackfillCursor(pctx, j.epoch); err != nil {
			cancel()
			for range pending {
			}
			<-dispatchDone
			<-producerDone
			return nextResume, fmt.Errorf("backfill: set cursor %d: %w", j.epoch, err)
		}
		nextResume = j.epoch + 1
	}

	<-dispatchDone
	if err := <-producerDone; err != nil {
		return nextResume, err
	}
	return nextResume, nil
}

// builtEpoch carries the fetch+build output of one epoch across the pipeline
// boundary. epochBS is nil when IPFS upload is disabled; empty (Blobs == 0)
// epochs are signalled with the empty sentinel and persisted as zero-blob rows.
type builtEpoch struct {
	epoch       uint64
	epochInp    types.EpochInput
	blobResults []types.BlobResult
	epochResult types.EpochResult
	epochBS     *store.MemBlockstore
	fromCache   bool
	empty       bool // epoch had no blobs; a zero-blob EpochNode is built at persist time
	skip        bool // epoch already in DB for this network; do not persist
}

// buildEpoch performs the CPU + beacon-bound stage of processing one epoch:
// DB-cache check → beacon fetch → blob processing → BuildEpochNode. It does
// not touch IPFS or write the DB. Returns a builtEpoch ready to be persisted
// or, when the epoch has no blobs, one with empty=true.
func (g *Generator) buildEpoch(ctx context.Context, epoch uint64, p *epochProgress) (builtEpoch, error) {
	out := builtEpoch{epoch: epoch}

	var (
		epochInp    types.EpochInput
		blobResults []types.BlobResult
		fromCache   bool
	)

	var epochBS *store.MemBlockstore
	var lsys ipld.LinkSystem
	if g.ipfs != nil {
		epochBS = store.NewMemBlockstore()
		lsys = store.NewLinkSystem(epochBS)
	} else {
		lsys = store.NewLinkSystem(store.NullBlockstore{})
	}

	if g.db != nil {
		cached, err := g.db.GetBlobsByEpoch(ctx, g.cfg.Network.Name, epoch)
		if err != nil {
			return out, fmt.Errorf("check cached blobs epoch %d: %w", epoch, err)
		}
		if len(cached) > 0 {
			g.log.Debug("using cached blobs from DB, skipping beacon fetch",
				"epoch", epoch, "blobs", len(cached))
			epochInp, blobResults, err = g.reconstructFromDB(epoch, cached)
			if err != nil {
				return out, fmt.Errorf("reconstruct epoch %d from DB: %w", epoch, err)
			}
			fromCache = true
		}
	}

	if !fromCache {
		g.logFetchingEpoch(epoch, p)
		var err error
		epochInp, err = g.beacon.FetchEpochInput(ctx, epoch, nil)
		if err != nil {
			return out, fmt.Errorf("fetch epoch %d: %w", epoch, err)
		}

		if len(epochInp.Blobs) == 0 {
			g.log.Info("epoch has no blobs, skipping", "epoch", epoch)
			out.empty = true
			return out, nil
		}

		g.log.Debug("blobs fetched from beacon",
			"epoch", epoch,
			"blobs", len(epochInp.Blobs),
			"first_slot", epochInp.Slot,
			"last_slot", epochInp.Slot+31,
		)

		blobResults, err = g.processEpochBlobs(ctx, epochInp, lsys)
		if err != nil {
			return out, fmt.Errorf("process blobs epoch %d: %w", epoch, err)
		}
	}

	epochResult, err := builder.BuildEpochNode(
		ctx, lsys, epochInp, blobResults,
		g.cfg.Network.Name,
		g.cfg.Generator.HAMTThreshold,
	)
	if err != nil {
		return out, fmt.Errorf("build epoch node %d: %w", epoch, err)
	}

	srcIcon := "◆"
	if p != nil {
		switch p.src {
		case "live":
			srcIcon = "●"
		case "backfill":
			srcIcon = "■"
		}
	}
	var rpcCount int64
	if g.beacon != nil {
		rpcCount = g.beacon.GetRPCRequestCount()
	}
	g.log.Info(fmt.Sprintf("%s Epoch %d built [%d blobs]", srcIcon, epoch, len(blobResults)),
		"cid", epochResult.CID.String(),
		"rpc_requests", rpcCount)

	out.epochInp = epochInp
	out.blobResults = blobResults
	out.epochResult = epochResult
	out.epochBS = epochBS
	out.fromCache = fromCache
	return out, nil
}

// persistEpoch performs the I/O-bound stage: IPFS upload → DB save → notifier.
// It is safe to call concurrently with another epoch's buildEpoch as long as
// callers serialize persistEpoch invocations to preserve cursor monotonicity.
func (g *Generator) persistEpoch(ctx context.Context, be builtEpoch) error {
	if be.empty {
		if g.db == nil {
			return nil
		}
		// Epoch had no blobs (e.g. early post-Dencun epochs). Build a zero-blob
		// EpochNode so the DB row exists and gap detection doesn't flag it.
		var lsys ipld.LinkSystem
		var emptyBS *store.MemBlockstore
		if g.ipfs != nil {
			emptyBS = store.NewMemBlockstore()
			lsys = store.NewLinkSystem(emptyBS)
		} else {
			lsys = store.NewLinkSystem(store.NullBlockstore{})
		}
		emptyInp := types.EpochInput{Epoch: be.epoch, Slot: beacon.EpochToFirstSlot(be.epoch, g.slotsPerEpoch)}
		epochResult, err := builder.BuildEpochNode(ctx, lsys, emptyInp, nil, g.cfg.Network.Name, g.cfg.Generator.HAMTThreshold)
		if err != nil {
			return fmt.Errorf("build empty epoch node %d: %w", be.epoch, err)
		}
		if emptyBS != nil {
			if err := g.uploadAndPin(ctx, emptyBS, epochResult.CID, be.epoch); err != nil {
				return err
			}
		}
		epochTime := beacon.SlotTime(g.genesisTime, beacon.EpochToFirstSlot(be.epoch, g.slotsPerEpoch), g.secondsPerSlot)
		if err := g.db.SaveEpoch(ctx, g.cfg.Network.Name, epochResult, 0, epochTime); err != nil {
			return fmt.Errorf("save empty epoch %d to db: %w", be.epoch, err)
		}
		g.log.Info("empty epoch saved", "epoch", be.epoch, "cid", epochResult.CID)
		return nil
	}
	if be.epochBS != nil {
		if err := g.uploadAndPin(ctx, be.epochBS, be.epochResult.CID, be.epoch); err != nil {
			return err
		}
	}

	if g.db != nil {
		g.log.Debug("saving to database", "epoch", be.epoch, "blobs", len(be.blobResults))
		epochTime := beacon.SlotTime(g.genesisTime, beacon.EpochToFirstSlot(be.epoch, g.slotsPerEpoch), g.secondsPerSlot)
		if err := g.db.SaveEpoch(ctx, g.cfg.Network.Name, be.epochResult, len(be.blobResults), epochTime); err != nil {
			return fmt.Errorf("save epoch %d to db: %w", be.epoch, err)
		}
		if !be.fromCache {
			if err := g.db.SaveBlobs(ctx, g.cfg.Network.Name, be.epoch, be.epochInp.Blobs, be.blobResults, g.genesisTime, g.secondsPerSlot); err != nil {
				return fmt.Errorf("save blobs epoch %d to db: %w", be.epoch, err)
			}
		}
	}

	if !be.fromCache {
		g.notifier.NotifyBlobs(ctx, buildReferences(be.epochInp.Blobs, be.blobResults))
	}

	g.log.Debug("epoch complete", "epoch", be.epoch)
	return nil
}

// processEpoch is the sequential (non-pipelined) helper used by live mode and
// single-epoch CLI commands. The backfill loop uses buildEpoch + persistEpoch
// directly to overlap fetch(N+1) with persist(N).
func (g *Generator) processEpoch(ctx context.Context, epoch uint64, p *epochProgress) error {
	if g.ipfs == nil && g.db == nil {
		return nil
	}
	g.log.Debug("processing epoch", "epoch", epoch)
	be, err := g.buildEpoch(ctx, epoch, p)
	if err != nil {
		return err
	}
	return g.persistEpoch(ctx, be)
}

// reconstructFromDB rebuilds EpochInput and BlobResult slices from DB records,
// avoiding a full beacon re-fetch. The raw blob Data is not loaded (not needed
// for BuildEpochNode when BlobResult CIDs are already known).
func (g *Generator) reconstructFromDB(epoch uint64, records []db.BlobRecord) (types.EpochInput, []types.BlobResult, error) {
	return ReconstructFromDB(epoch, records, g.slotsPerEpoch)
}

// ReconstructFromDB maps DB blob rows for an epoch into the EpochInput and
// []BlobResult that builder.BuildEpochNode / StoreBlobMetadata consume, without
// re-fetching from the beacon. The raw blob Data is intentionally not loaded:
// Size carries the byte length so MetaCID stays identical, and the precomputed
// Data/Meta CIDs come straight from the rows.
//
// It is a pure function (no generator/beacon/IPFS state) so read-only callers —
// e.g. the health-check command — can reuse it without constructing a full
// Generator. The reconstructed nodes match the DB rows, so comparing the result
// against stored CIDs detects metadata corruption.
func ReconstructFromDB(epoch uint64, records []db.BlobRecord, slotsPerEpoch uint64) (types.EpochInput, []types.BlobResult, error) {
	firstSlot := beacon.EpochToFirstSlot(epoch, slotsPerEpoch)
	epochInp := types.EpochInput{
		Epoch: epoch,
		Slot:  firstSlot,
	}
	blobResults := make([]types.BlobResult, len(records))

	for i, r := range records {
		epochInp.Blobs = append(epochInp.Blobs, types.BlobInput{
			Commitment:    r.Commitment,
			VersionedHash: r.VersionedHash,
			TxHash:        r.TxHash,
			BlockNumber:   r.BlockNumber,
			BlockHash:     r.BlockHash,
			Slot:          r.Slot,
			Epoch:         epoch,
			Index:         r.BlobIndex,
			Size:          r.SizeBytes, // Data is not loaded; Size ensures correct MetaCID
		})

		dataCID, err := cid.Decode(r.DataCID)
		if err != nil {
			return types.EpochInput{}, nil, fmt.Errorf("decode data cid %q: %w", r.DataCID, err)
		}
		metaCID, err := cid.Decode(r.MetaCID)
		if err != nil {
			return types.EpochInput{}, nil, fmt.Errorf("decode meta cid %q: %w", r.MetaCID, err)
		}
		blobResults[i] = types.BlobResult{
			Commitment: r.Commitment,
			DataCID:    dataCID,
			MetaCID:    metaCID,
			SizeBytes:  r.SizeBytes,
		}
	}

	return epochInp, blobResults, nil
}

// ─── Push API entry point ─────────────────────────────────────────────────────

// ProcessBlobInput is the BlobProcessor implementation used by the HTTP API.
// It processes a single blob pushed from an external source (not the beacon node).
// If this is a new epoch (first blob for that epoch), an EpochNode is not built
// yet — it is built when all blobs for the epoch have arrived and
// FinalizeEpoch is called. For simplicity in the push model, each individual
// blob is stored and uploaded immediately, and the EpochNode is rebuilt on
// FinalizeEpoch.
func (g *Generator) ProcessBlobInput(ctx context.Context, req api.BlobPushRequest) (api.BlobPushResponse, error) {
	rawData, err := hexDecode(req.Data)
	if err != nil {
		return api.BlobPushResponse{}, fmt.Errorf("decode blob data: %w", err)
	}

	inp := types.BlobInput{
		Commitment:    req.Commitment,
		VersionedHash: req.VersionedHash,
		TxHash:        req.TxHash,
		BlockNumber:   req.BlockNumber,
		BlockHash:     req.BlockHash,
		Slot:          req.Slot,
		Epoch:         req.Epoch,
		Index:         req.Index,
		Data:          rawData,
	}

	var blobBS *store.MemBlockstore
	var lsys ipld.LinkSystem
	if g.ipfs == nil {
		lsys = store.NewLinkSystem(store.NullBlockstore{})
	} else {
		blobBS = store.NewMemBlockstore()
		lsys = store.NewLinkSystem(blobBS)
	}

	res, err := builder.ProcessBlob(ctx, lsys, inp)
	if err != nil {
		return api.BlobPushResponse{}, fmt.Errorf("process blob: %w", err)
	}

	// Upload just this blob's blocks to IPFS (skipped when ipfs is nil).
	if blobBS != nil {
		if err := g.ipfs.PutBlockstore(ctx, blobBS); err != nil {
			return api.BlobPushResponse{}, fmt.Errorf("upload blob to ipfs: %w", err)
		}
	}

	// Persist blob record to DB (epoch row may not exist yet; SaveBlobs handles that).
	if g.db != nil {
		if err := g.db.SaveBlobs(ctx, g.cfg.Network.Name, req.Epoch, []types.BlobInput{inp}, []types.BlobResult{res}, g.genesisTime, g.secondsPerSlot); err != nil {
			return api.BlobPushResponse{}, fmt.Errorf("save blob to db: %w", err)
		}
	}

	g.log.Info("blob pushed and stored",
		"commitment", req.Commitment,
		"epoch", req.Epoch,
		"data_cid", res.DataCID,
		"meta_cid", res.MetaCID,
	)

	g.notifier.NotifyBlobs(ctx, []blobscan.BlobReference{{
		VersionedHash: req.VersionedHash,
		DataCID:       res.DataCID.String(),
		MetaCID:       res.MetaCID.String(),
	}})

	return api.BlobPushResponse{
		DataCID:    res.DataCID.String(),
		MetaCID:    res.MetaCID.String(),
		Commitment: res.Commitment,
		Epoch:      req.Epoch,
	}, nil
}

// FinalizeEpochWithCID is identical to FinalizeEpoch but also returns the
// EpochNode CID string. Used by the HTTP API's EpochFinalizer callback.
func (g *Generator) FinalizeEpochWithCID(ctx context.Context, epoch uint64) (string, error) {
	epochCID, err := g.finalizeEpochInner(ctx, epoch)
	if err != nil {
		return "", err
	}
	return epochCID.String(), nil
}

// FinalizeEpoch builds and uploads the EpochNode for the given epoch using all
// blobs already stored in the DB, then rebuilds the NetworkRoot.
// Call this via the CLI or after all blobs for an epoch have been pushed.
func (g *Generator) FinalizeEpoch(ctx context.Context, epoch uint64) error {
	_, err := g.finalizeEpochInner(ctx, epoch)
	return err
}

// RepairEpochs rebuilds epoch nodes from the DB cache for all epochs that have
// blob_count=0 in ipld_epochs but real rows in ipld_blobs. It uploads to IPFS
// once per epoch but defers the expensive network root rebuild to the end.
//
// repair rebuilds from DB metadata *without* re-fetching blob data, so it cannot
// detect when that metadata is itself wrong — it would faithfully compute and
// persist a wrong epoch root. When strict is true, after uploading we verify the
// whole DAG resolves locally (VerifyLocal); if a referenced block is absent the
// epoch is failed rather than saved, with a message pointing at backfill-ipfs
// (which re-fetches real data and self-heals the DB). Pass strict=false to keep
// the old trust-the-DB behavior.
func (g *Generator) RepairEpochs(ctx context.Context, epochs []uint64, strict bool, onDone func(epoch uint64, err error)) (int, int) {
	ok, failed := 0, 0
	for i, epoch := range epochs {
		if ctx.Err() != nil {
			break
		}
		g.log.Info("repairing epoch", "epoch", epoch, "progress", fmt.Sprintf("%d/%d", i+1, len(epochs)))
		if g.db == nil {
			onDone(epoch, fmt.Errorf("DB not configured"))
			failed++
			continue
		}
		blobs, err := g.db.GetBlobsByEpoch(ctx, g.cfg.Network.Name, epoch)
		if err != nil {
			onDone(epoch, fmt.Errorf("load blobs: %w", err))
			failed++
			continue
		}
		if len(blobs) == 0 {
			onDone(epoch, fmt.Errorf("no blobs in DB"))
			failed++
			continue
		}
		epochInp, blobResults, err := g.reconstructFromDB(epoch, blobs)
		if err != nil {
			onDone(epoch, fmt.Errorf("reconstruct: %w", err))
			failed++
			continue
		}
		epochBS := store.NewMemBlockstore()
		lsys := store.NewLinkSystem(epochBS)
		epochResult, err := builder.BuildEpochNode(ctx, lsys, epochInp, blobResults,
			g.cfg.Network.Name, g.cfg.Generator.HAMTThreshold)
		if err != nil {
			onDone(epoch, fmt.Errorf("build node: %w", err))
			failed++
			continue
		}
		if err := g.uploadAndPin(ctx, epochBS, epochResult.CID, epoch); err != nil {
			onDone(epoch, fmt.Errorf("ipfs upload: %w", err))
			failed++
			continue
		}
		// Integrity guard: confirm the rebuilt DAG resolves entirely from the
		// local datastore. A missing block means the DB metadata we rebuilt from
		// is wrong or the blob data was never uploaded — saving this root would
		// record a bad CID. Fail the epoch instead and point at backfill-ipfs.
		if strict && g.ipfs != nil {
			vctx, vcancel := context.WithTimeout(ctx, 30*time.Second)
			verr := g.ipfs.VerifyLocal(vctx, epochResult.CID)
			vcancel()
			if verr != nil {
				onDone(epoch, fmt.Errorf("integrity check failed (DB metadata likely wrong or blob data missing) — run `backfill-ipfs -from %d -to %d` to re-fetch and self-heal: %w", epoch, epoch, verr))
				failed++
				continue
			}
		}
		// Persist the corrected row using a context detached from ctx's
		// cancellation: an interrupt during the (best-effort) pin above must not
		// lose the durable repair we just computed and uploaded. The blob_count
		// fix is the whole point of repair-epochs; never drop it to a cancel.
		saveCtx, saveCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		epochTime := beacon.SlotTime(g.genesisTime, beacon.EpochToFirstSlot(epoch, g.slotsPerEpoch), g.secondsPerSlot)
		err = g.db.SaveEpoch(saveCtx, g.cfg.Network.Name, epochResult, len(blobs), epochTime)
		saveCancel()
		if err != nil {
			onDone(epoch, fmt.Errorf("save epoch: %w", err))
			failed++
			continue
		}
		onDone(epoch, nil)
		ok++
	}

	// Rebuild network root once after all epochs are repaired.
	if ok > 0 {
		if err := g.rebuildNetworkRoot(ctx); err != nil {
			g.log.Warn("network root rebuild failed after repair (non-fatal)", "err", err)
		}
	}
	return ok, failed
}

// finalizeEpochInner contains the shared implementation; returns the EpochNode CID.
// Requires DB persistence to be enabled (blobs must have been saved via SaveBlobs).
func (g *Generator) finalizeEpochInner(ctx context.Context, epoch uint64) (cid.Cid, error) {
	if g.db == nil {
		return cid.Cid{}, fmt.Errorf("finalize epoch %d: DB persistence is disabled (postgres_dsn not set); cannot reconstruct blobs", epoch)
	}
	blobs, err := g.db.GetBlobsByEpoch(ctx, g.cfg.Network.Name, epoch)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("load blobs epoch %d: %w", epoch, err)
	}
	if len(blobs) == 0 {
		return cid.Cid{}, fmt.Errorf("no blobs found for epoch %d", epoch)
	}

	epochInp, blobResults, err := g.reconstructFromDB(epoch, blobs)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("reconstruct epoch %d from DB: %w", epoch, err)
	}

	epochBS := store.NewMemBlockstore()
	lsys := store.NewLinkSystem(epochBS)

	epochResult, err := builder.BuildEpochNode(
		ctx, lsys, epochInp, blobResults,
		g.cfg.Network.Name,
		g.cfg.Generator.HAMTThreshold,
	)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("build epoch node %d: %w", epoch, err)
	}

	if err := g.uploadAndPin(ctx, epochBS, epochResult.CID, epoch); err != nil {
		return cid.Cid{}, err
	}

	epochTime := beacon.SlotTime(g.genesisTime, beacon.EpochToFirstSlot(epoch, g.slotsPerEpoch), g.secondsPerSlot)
	if err := g.db.SaveEpoch(ctx, g.cfg.Network.Name, epochResult, len(blobs), epochTime); err != nil {
		return cid.Cid{}, fmt.Errorf("save epoch %d to db: %w", epoch, err)
	}

	if err := g.rebuildNetworkRoot(ctx); err != nil {
		g.log.Warn("network root rebuild failed (non-fatal)", "epoch", epoch, "err", err)
	}

	if err := g.state.SetLastProcessedEpoch(ctx, epoch); err != nil {
		return cid.Cid{}, err
	}
	return epochResult.CID, nil
}

// ─── Internal helpers ─────────────────────────────────────────────────────────

func (g *Generator) uploadAndPin(ctx context.Context, bs *store.MemBlockstore, epochCID cid.Cid, epoch uint64) error {
	if g.ipfs == nil {
		return nil
	}
	total := bs.Len()
	g.log.Debug("uploading blocks to IPFS", "epoch", epoch, "blocks", total)
	progress := func(current, count int, blockCID string) {
		if current == count || current%10 == 0 {
			g.log.Debug("IPFS upload progress", "epoch", epoch, "blocks", fmt.Sprintf("%d/%d", current, count), "cid", blockCID)
		}
	}
	if err := g.ipfs.PutBlockstore(ctx, bs, progress); err != nil {
		return fmt.Errorf("upload epoch %d to IPFS: %w", epoch, err)
	}
	g.log.Debug("IPFS upload complete", "epoch", epoch, "blocks", total)
	if g.cfg.IPFS.PinOnAdd {
		// Verify the whole DAG is present locally before pinning. A recursive
		// pin will otherwise block fetching any missing child over the network
		// (potentially forever). The data blocks are expected to already be on
		// the node, so a missing block is a real problem we want surfaced fast,
		// not a network wait. Bounded with a short timeout independent of ctx so
		// an interrupt here cannot poison the caller's context.
		checkCtx, checkCancel := context.WithTimeout(ctx, 30*time.Second)
		verifyErr := g.ipfs.VerifyLocal(checkCtx, epochCID)
		checkCancel()
		if verifyErr != nil {
			g.log.Warn("skipping pin: epoch DAG not fully present locally (non-fatal)",
				"epoch", epoch, "cid", epochCID, "err", verifyErr)
			return nil
		}
		pinCtx, pinCancel := context.WithTimeout(ctx, 30*time.Second)
		err := g.ipfs.Pin(pinCtx, epochCID)
		pinCancel()
		if err != nil {
			g.log.Warn("pin epoch failed (non-fatal)", "epoch", epoch, "cid", epochCID, "err", err)
		}
	}
	return nil
}

// rebuildNetworkRoot loads all known epochs from the DB and builds a new
// NetworkRoot node, then uploads it to IPFS. Called after every epoch.
// Returns nil without doing anything when DB persistence is disabled.
func (g *Generator) rebuildNetworkRoot(ctx context.Context) error {
	if g.db == nil {
		g.log.Debug("skipping NetworkRoot rebuild: DB persistence disabled")
		return nil
	}
	if g.ipfs == nil {
		g.log.Debug("skipping NetworkRoot rebuild: IPFS upload disabled")
		return nil
	}
	records, err := g.db.GetAllEpochs(ctx, g.cfg.Network.Name)
	if err != nil {
		return fmt.Errorf("load epochs for network root: %w", err)
	}

	epochResults := make([]types.EpochResult, len(records))
	for i, r := range records {
		c, err := cid.Decode(r.CID)
		if err != nil {
			return fmt.Errorf("decode epoch cid %q: %w", r.CID, err)
		}
		epochResults[i] = types.EpochResult{
			Epoch:                r.Epoch,
			CID:                  c,
			ApproximateSizeBytes: r.SizeBytes,
		}
	}

	rootBS := store.NewMemBlockstore()
	lsys := store.NewLinkSystem(rootBS)

	rootResult, err := builder.BuildNetworkRoot(ctx, lsys, g.cfg.Network.Name, epochResults,
		g.cfg.Generator.NetworkRootPageSize)
	if err != nil {
		return fmt.Errorf("build network root: %w", err)
	}

	if err := g.ipfs.PutBlockstore(ctx, rootBS); err != nil {
		return fmt.Errorf("upload network root to IPFS: %w", err)
	}

	g.log.Debug("network root rebuilt", "cid", rootResult.CID, "epochs", len(epochResults),
		"pages", rootResult.PageCount)
	return nil
}

// processEpochBlobs processes all blobs in an epoch concurrently using a worker pool.
// lsys is provided by the caller; use NullBlockstore-backed lsys when upload is skipped.
func (g *Generator) processEpochBlobs(ctx context.Context, epochInp types.EpochInput, lsys ipld.LinkSystem) ([]types.BlobResult, error) {
	type job struct {
		idx  int
		blob types.BlobInput
	}
	type result struct {
		idx int
		res types.BlobResult
		err error
	}

	jobs := make(chan job, len(epochInp.Blobs))
	results := make(chan result, len(epochInp.Blobs))

	workers := g.cfg.Generator.Workers
	if workers > len(epochInp.Blobs) {
		workers = len(epochInp.Blobs)
	}

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				res, err := builder.ProcessBlob(ctx, lsys, j.blob)
				results <- result{idx: j.idx, res: res, err: err}
			}
		}()
	}

	for i, b := range epochInp.Blobs {
		jobs <- job{idx: i, blob: b}
	}
	close(jobs)

	go func() {
		wg.Wait()
		close(results)
	}()

	blobResults := make([]types.BlobResult, len(epochInp.Blobs))
	for r := range results {
		if r.err != nil {
			return nil, r.err
		}
		blobResults[r.idx] = r.res
		g.log.Debug("blob processed",
			"epoch", epochInp.Epoch,
			"slot", epochInp.Blobs[r.idx].Slot,
			"index", epochInp.Blobs[r.idx].Index,
			"commitment", epochInp.Blobs[r.idx].Commitment[:16]+"…",
			"data_cid", r.res.DataCID,
			"meta_cid", r.res.MetaCID,
		)
	}

	return blobResults, nil
}

// ProcessSingleEpoch is a one-shot helper for backfilling or manual invocation.
func (g *Generator) ProcessSingleEpoch(ctx context.Context, epoch uint64) error {
	if err := g.processEpoch(ctx, epoch, nil); err != nil {
		return err
	}
	if err := g.state.SetLastProcessedEpoch(ctx, epoch); err != nil {
		return err
	}
	if err := g.rebuildNetworkRoot(ctx); err != nil {
		g.log.Warn("network root rebuild failed (non-fatal)", "epoch", epoch, "err", err)
	}
	return nil
}

// BackfillIPFS re-fetches blob data from the beacon node for every epoch in
// [fromEpoch, toEpoch] and uploads all IPLD blocks to IPFS.
//
// This is the recovery path for deployments that ran with skip_upload=true:
// the DB already contains correct CIDs (they are deterministic hashes), but the
// actual blocks were discarded into NullBlockstore and never reached IPFS.
//
// For each epoch:
//  1. Load DB blob records (skip with a warning if none found — epoch not indexed).
//  2. Re-fetch blob sidecars from the beacon node with real data.
//  3. Re-run the full ProcessBlob pipeline into a fresh MemBlockstore.
//  4. Compare newly computed CIDs against the DB values; log an error on any mismatch.
//  5. Build the EpochNode (also freshly, so its blocks land in the MemBlockstore).
//  6. Upload everything to IPFS and update the epoch row in the DB.
//
// A single beacon error on one epoch is logged and skipped rather than aborting
// the whole run, so a transient RPC failure doesn't restart a long backfill.
// NetworkRoot is rebuilt once at the end.
//
// Requires both IPFS and DB to be configured.
func (g *Generator) BackfillIPFS(ctx context.Context, fromEpoch, toEpoch uint64) error {
	if g.ipfs == nil {
		return fmt.Errorf("backfill-ipfs: IPFS client is not configured (skip_upload=true?)")
	}
	if g.db == nil {
		return fmt.Errorf("backfill-ipfs: DB is not configured (postgres_dsn not set)")
	}
	if g.beacon == nil {
		return fmt.Errorf("backfill-ipfs: beacon_rpc is required to re-fetch blob data")
	}
	if toEpoch < fromEpoch {
		return fmt.Errorf("backfill-ipfs: to_epoch (%d) must be >= from_epoch (%d)", toEpoch, fromEpoch)
	}

	total := int(toEpoch-fromEpoch) + 1
	g.log.Info(fmt.Sprintf("⟲ IPFS backfill: %d epoch%s [%d → %d]", total, pluralize(total), fromEpoch, toEpoch))

	batchStart := time.Now()
	skipped := 0
	uploaded := 0

	for epoch := fromEpoch; epoch <= toEpoch; epoch++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		p := &epochProgress{
			idx:        int(epoch - fromEpoch),
			total:      total,
			batchStart: batchStart,
			src:        "backfill-ipfs",
		}

		// Load DB records — if none exist the epoch was never indexed; skip.
		dbBlobs, err := g.db.GetBlobsByEpoch(ctx, g.cfg.Network.Name, epoch)
		if err != nil {
			return fmt.Errorf("backfill-ipfs: load blobs epoch %d: %w", epoch, err)
		}
		if len(dbBlobs) == 0 {
			g.log.Warn("backfill-ipfs: no blobs in DB for epoch, skipping", "epoch", epoch)
			continue
		}

		// Re-fetch fresh blob data from the beacon node.
		g.logFetchingEpoch(epoch, p)
		epochInp, err := g.beacon.FetchEpochInput(ctx, epoch, nil)
		if err != nil {
			g.log.Error("backfill-ipfs: beacon fetch failed, skipping epoch", "epoch", epoch, "err", err)
			skipped++
			continue
		}

		if len(epochInp.Blobs) == 0 {
			g.log.Warn("backfill-ipfs: beacon returned no blobs for epoch", "epoch", epoch)
			skipped++
			continue
		}

		// Build blob blocks into a fresh MemBlockstore with real data.
		epochBS := store.NewMemBlockstore()
		lsys := store.NewLinkSystem(epochBS)

		blobResults, err := g.processEpochBlobs(ctx, epochInp, lsys)
		if err != nil {
			g.log.Error("backfill-ipfs: blob processing failed, skipping epoch", "epoch", epoch, "err", err)
			skipped++
			continue
		}

		// Verify freshly-computed CIDs against DB values.
		// Build a lookup map: commitment → DB record.
		dbByCommitment := make(map[string]db.BlobRecord, len(dbBlobs))
		for _, r := range dbBlobs {
			dbByCommitment[r.Commitment] = r
		}

		cidMismatch := false
		for i, res := range blobResults {
			commitment := epochInp.Blobs[i].Commitment
			dbRec, ok := dbByCommitment[commitment]
			if !ok {
				g.log.Error("backfill-ipfs: blob from beacon not found in DB",
					"epoch", epoch, "commitment", commitment)
				cidMismatch = true
				continue
			}
			if res.DataCID.String() != dbRec.DataCID {
				g.log.Error("backfill-ipfs: DataCID mismatch",
					"epoch", epoch,
					"commitment", commitment,
					"db_cid", dbRec.DataCID,
					"computed_cid", res.DataCID.String(),
				)
				cidMismatch = true
			}
			if res.MetaCID.String() != dbRec.MetaCID {
				g.log.Error("backfill-ipfs: MetaCID mismatch",
					"epoch", epoch,
					"commitment", commitment,
					"db_cid", dbRec.MetaCID,
					"computed_cid", res.MetaCID.String(),
				)
				cidMismatch = true
			}
		}
		if cidMismatch {
			g.log.Warn("backfill-ipfs: CID mismatches detected — uploading freshly computed blocks (DB will be updated)",
				"epoch", epoch)
		}

		// Build epoch node so its blocks also land in epochBS.
		epochResult, err := builder.BuildEpochNode(
			ctx, lsys, epochInp, blobResults,
			g.cfg.Network.Name,
			g.cfg.Generator.HAMTThreshold,
		)
		if err != nil {
			g.log.Error("backfill-ipfs: build epoch node failed, skipping epoch", "epoch", epoch, "err", err)
			skipped++
			continue
		}

		var rpcCount int64
		if g.beacon != nil {
			rpcCount = g.beacon.GetRPCRequestCount()
		}
		g.log.Info(fmt.Sprintf("■ Epoch %d rebuilt [%d blobs]", epoch, len(blobResults)),
			"cid", epochResult.CID.String(),
			"rpc_requests", rpcCount,
		)

		// Upload all blocks (blob data + metadata + epoch node + HAMT) to IPFS.
		// Retry indefinitely on transient errors (e.g. IPFS node not yet reachable).
		// block/put is idempotent on Kubo, so retrying a partial upload is safe.
		for {
			err := g.uploadAndPin(ctx, epochBS, epochResult.CID, epoch)
			if err == nil {
				break
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			g.log.Error("backfill-ipfs: IPFS upload failed, retrying", "epoch", epoch, "err", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(g.cfg.Generator.PollInterval):
			}
		}

		// Persist/update epoch row (idempotent ON CONFLICT DO UPDATE).
		epochTime := beacon.SlotTime(g.genesisTime, beacon.EpochToFirstSlot(epoch, g.slotsPerEpoch), g.secondsPerSlot)
		if err := g.db.SaveEpoch(ctx, g.cfg.Network.Name, epochResult, len(blobResults), epochTime); err != nil {
			return fmt.Errorf("backfill-ipfs: save epoch %d: %w", epoch, err)
		}
		// Only update blob rows when CIDs differed (saves unnecessary DB writes on clean runs).
		if cidMismatch {
			if err := g.db.SaveBlobs(ctx, g.cfg.Network.Name, epoch, epochInp.Blobs, blobResults, g.genesisTime, g.secondsPerSlot); err != nil {
				return fmt.Errorf("backfill-ipfs: save blobs epoch %d: %w", epoch, err)
			}
		}

		if err := g.state.SetBackfillCursor(ctx, epoch); err != nil {
			return fmt.Errorf("backfill-ipfs: set cursor %d: %w", epoch, err)
		}
		uploaded++
	}

	g.log.Info("backfill-ipfs complete",
		"from", fromEpoch, "to", toEpoch,
		"uploaded", uploaded, "skipped", skipped,
	)

	if err := g.rebuildNetworkRoot(ctx); err != nil {
		g.log.Warn("backfill-ipfs: final network root rebuild failed (non-fatal)", "err", err)
	}
	return nil
}

// logFetchingEpoch logs the "fetching blobs from beacon" line enriched with
// block timestamp, indexing speed, and estimated time to finish the batch.
func (g *Generator) logFetchingEpoch(epoch uint64, p *epochProgress) {
	args := []any{"epoch", epoch}

	if p != nil && p.src != "" {
		args = append(args, "src", p.src)
	}

	if !g.genesisTime.IsZero() {
		firstSlot := beacon.EpochToFirstSlot(epoch, g.slotsPerEpoch)
		blockTime := beacon.SlotTime(g.genesisTime, firstSlot, g.secondsPerSlot)
		args = append(args, "block_time", blockTime.Format(time.RFC3339))
	}

	if p != nil && p.idx > 0 {
		elapsed := time.Since(p.batchStart)
		speed := float64(p.idx) / elapsed.Seconds() // epochs/s
		remaining := p.total - p.idx
		eta := time.Duration(float64(remaining)/speed) * time.Second
		args = append(args,
			"progress", fmt.Sprintf("%d/%d", p.idx+1, p.total),
			"speed", fmt.Sprintf("%.2f epochs/s", speed),
			"eta", eta.Round(time.Second).String(),
		)
	} else if p != nil {
		args = append(args, "progress", fmt.Sprintf("%d/%d", p.idx+1, p.total))
	}

	g.log.Info("fetching blobs from beacon", args...)
}

// hexDecode accepts 0x-prefixed or plain hex strings.
func hexDecode(s string) ([]byte, error) {
	s = strings.TrimPrefix(s, "0x")
	out := make([]byte, hex.DecodedLen(len(s)))
	n, err := hex.Decode(out, []byte(s))
	return out[:n], err
}

// pluralize returns the plural suffix for the given count.
func pluralize(count int) string {
	if count == 1 {
		return ""
	}
	return "s"
}

// buildReferences zips BlobInputs and BlobResults into BlobReference slices for
// the blobscan notifier.
func buildReferences(blobs []types.BlobInput, results []types.BlobResult) []blobscan.BlobReference {
	refs := make([]blobscan.BlobReference, len(blobs))
	for i := range blobs {
		refs[i] = blobscan.BlobReference{
			VersionedHash: blobs[i].VersionedHash,
			DataCID:       results[i].DataCID.String(),
			MetaCID:       results[i].MetaCID.String(),
		}
	}
	return refs
}
