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
	cfg    *config.Config
	beacon *beacon.Client // nil when beacon_rpc is not configured
	ipfs   *ipfs.Client
	db     *db.Client
	state  state.Backend
	log    *slog.Logger
}

// New creates a new Generator from the given configuration.
func New(ctx context.Context, cfg *config.Config, log *slog.Logger) (*Generator, error) {
	var beaconClient *beacon.Client
	if cfg.Network.BeaconRPC != "" {
		beaconClient = beacon.NewClient(cfg.Network.BeaconRPC, cfg.IPFS.Timeout)
	}

	var ipfsClient *ipfs.Client
	if !cfg.IPFS.SkipUpload {
		var err error
		ipfsClient, err = ipfs.NewClient(cfg.IPFS.APIAddr, cfg.IPFS.Timeout, cfg.IPFS.PinOnAdd)
		if err != nil {
			return nil, fmt.Errorf("generator: create ipfs client: %w", err)
		}
	} else {
		log.Info("IPFS upload disabled (skip_upload=true); CIDs will be computed but not uploaded")
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
		// DB is available: use it as the state backend. MAX(epoch) on ipld_epochs
		// is the resume point — no separate state file needed.
		stateBackend = db.NewDBStateBackend(dbClient, cfg.Network.Name)
		log.Info("using DB state backend")
	} else {
		// Fall back to the JSON file-based state manager.
		stateMgr, err := state.NewManager(cfg.Storage.DataDir, cfg.Network.Name)
		if err != nil {
			return nil, fmt.Errorf("generator: load state: %w", err)
		}
		stateBackend = stateMgr
		log.Info("using file state backend", "path", cfg.Storage.DataDir)
	}

	return &Generator{
		cfg:    cfg,
		beacon: beaconClient,
		ipfs:   ipfsClient,
		db:     dbClient,
		state:  stateBackend,
		log:    log,
	}, nil
}

// Close releases resources held by the generator (DB pool, etc.).
func (g *Generator) Close() {
	if g.db != nil {
		g.db.Close()
	}
}

// Run starts the main beacon-pull processing loop. It blocks until ctx is cancelled.
// Requires beacon_rpc to be configured.
func (g *Generator) Run(ctx context.Context) error {
	if g.beacon == nil {
		return fmt.Errorf("generator: beacon_rpc is required for the pull loop; use the push API instead")
	}

	g.log.Info("generator starting",
		"network", g.cfg.Network.Name,
		"poll_interval", g.cfg.Generator.PollInterval,
	)

	ticker := time.NewTicker(g.cfg.Generator.PollInterval)
	defer ticker.Stop()

	if err := g.processNewEpochs(ctx); err != nil {
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
			if err := g.processNewEpochs(ctx); err != nil {
				if errors.Is(err, beacon.ErrInsufficientCustody) {
					g.log.Error("aborting: beacon node cannot serve blob sidecars", "err", err)
					return err
				}
				g.log.Error("epoch processing failed", "err", err)
			}
		}
	}
}

// processNewEpochs checks for newly finalized epochs and processes each one.
func (g *Generator) processNewEpochs(ctx context.Context) error {
	checkpoints, err := g.beacon.GetFinalityCheckpoints(ctx, "head")
	if err != nil {
		return fmt.Errorf("generator: get finality checkpoints: %w", err)
	}

	lastProcessed, err := g.state.GetLastProcessedEpoch(ctx)
	if err != nil {
		return fmt.Errorf("generator: get last processed epoch: %w", err)
	}

	startEpoch := g.cfg.Generator.StartEpoch
	if g.cfg.Generator.SkipExistingEpochs && lastProcessed > 0 {
		startEpoch = lastProcessed + 1
	}

	finalizedEpoch := checkpoints.FinalizedEpoch
	if finalizedEpoch <= lastProcessed {
		g.log.Debug("no new finalized epochs", "finalized", finalizedEpoch, "last_processed", lastProcessed)
		return nil
	}

	g.log.Info("new finalized epochs detected", "from", startEpoch, "to", finalizedEpoch)

	for epoch := startEpoch; epoch <= finalizedEpoch; epoch++ {
		if err := g.processEpoch(ctx, epoch); err != nil {
			return fmt.Errorf("generator: process epoch %d: %w", epoch, err)
		}
	}

	return nil
}

// processEpoch handles a single finalized epoch end-to-end (beacon-pull mode):
// fetch → build blobs → build epoch node → upload IPFS → rebuild NetworkRoot → save DB.
// If blobs for this epoch already exist in the DB (e.g. from a previous interrupted run),
// the beacon fetch and blob processing are skipped entirely.
func (g *Generator) processEpoch(ctx context.Context, epoch uint64) error {
	g.log.Info("processing epoch", "epoch", epoch)

	var (
		epochInp    types.EpochInput
		blobResults []types.BlobResult
		epochBS     *store.MemBlockstore
		fromCache   bool
	)

	// Check DB for blobs already processed in a previous run.
	if g.db != nil {
		cached, err := g.db.GetBlobsByEpoch(ctx, epoch)
		if err != nil {
			return fmt.Errorf("check cached blobs epoch %d: %w", epoch, err)
		}
		if len(cached) > 0 {
			g.log.Info("using cached blobs from DB, skipping beacon fetch",
				"epoch", epoch, "blobs", len(cached))
			epochInp, blobResults, err = g.reconstructFromDB(epoch, cached)
			if err != nil {
				return fmt.Errorf("reconstruct epoch %d from DB: %w", epoch, err)
			}
			epochBS = store.NewMemBlockstore()
			fromCache = true
		}
	}

	if !fromCache {
		g.log.Info("fetching blobs from beacon", "epoch", epoch)
		var err error
		epochInp, err = g.beacon.FetchEpochInput(ctx, epoch, nil)
		if err != nil {
			return fmt.Errorf("fetch epoch %d: %w", epoch, err)
		}

		if len(epochInp.Blobs) == 0 {
			g.log.Info("epoch has no blobs, skipping", "epoch", epoch)
			return g.state.SetLastProcessedEpoch(ctx, epoch)
		}

		g.log.Info("blobs fetched from beacon",
			"epoch", epoch,
			"blobs", len(epochInp.Blobs),
			"first_slot", epochInp.Slot,
			"last_slot", epochInp.Slot+31,
		)

		g.log.Info("processing blobs", "epoch", epoch, "blobs", len(epochInp.Blobs))
		epochBS, blobResults, err = g.processEpochBlobs(ctx, epochInp)
		if err != nil {
			return fmt.Errorf("process blobs epoch %d: %w", epoch, err)
		}
	}

	lsys := store.NewLinkSystem(epochBS)

	epochResult, err := builder.BuildEpochNode(
		ctx, lsys, epochInp, blobResults,
		g.cfg.Network.Name,
		g.cfg.Generator.HAMTThreshold,
	)
	if err != nil {
		return fmt.Errorf("build epoch node %d: %w", epoch, err)
	}

	g.log.Info("epoch node built",
		"epoch", epoch,
		"cid", epochResult.CID,
		"blobs", len(blobResults),
		"size_bytes", epochResult.ApproximateSizeBytes,
	)

	if err := g.uploadAndPin(ctx, epochBS, epochResult.CID, epoch); err != nil {
		return err
	}

	if g.db != nil {
		g.log.Info("saving to database", "epoch", epoch, "blobs", len(blobResults))
		if err := g.db.SaveEpoch(ctx, g.cfg.Network.Name, epochResult, len(blobResults)); err != nil {
			return fmt.Errorf("save epoch %d to db: %w", epoch, err)
		}
		if !fromCache {
			if err := g.db.SaveBlobs(ctx, g.cfg.Network.Name, epoch, epochInp.Blobs, blobResults); err != nil {
				return fmt.Errorf("save blobs epoch %d to db: %w", epoch, err)
			}
		}
	}

	g.log.Info("rebuilding network root", "epoch", epoch)
	if err := g.rebuildNetworkRoot(ctx); err != nil {
		g.log.Warn("network root rebuild failed (non-fatal)", "epoch", epoch, "err", err)
	}

	g.log.Info("epoch complete", "epoch", epoch)
	return g.state.SetLastProcessedEpoch(ctx, epoch)
}

// reconstructFromDB rebuilds EpochInput and BlobResult slices from DB records,
// avoiding a full beacon re-fetch. The raw blob Data is not loaded (not needed
// for BuildEpochNode when BlobResult CIDs are already known).
func (g *Generator) reconstructFromDB(epoch uint64, records []db.BlobRecord) (types.EpochInput, []types.BlobResult, error) {
	firstSlot := epoch * 32
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
		if err := g.db.SaveBlobs(ctx, g.cfg.Network.Name, req.Epoch, []types.BlobInput{inp}, []types.BlobResult{res}); err != nil {
			return api.BlobPushResponse{}, fmt.Errorf("save blob to db: %w", err)
		}
	}

	g.log.Info("blob pushed and stored",
		"commitment", req.Commitment,
		"epoch", req.Epoch,
		"data_cid", res.DataCID,
		"meta_cid", res.MetaCID,
	)

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

// finalizeEpochInner contains the shared implementation; returns the EpochNode CID.
// Requires DB persistence to be enabled (blobs must have been saved via SaveBlobs).
func (g *Generator) finalizeEpochInner(ctx context.Context, epoch uint64) (cid.Cid, error) {
	if g.db == nil {
		return cid.Cid{}, fmt.Errorf("finalize epoch %d: DB persistence is disabled (postgres_dsn not set); cannot reconstruct blobs", epoch)
	}
	blobs, err := g.db.GetBlobsByEpoch(ctx, epoch)
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

	if err := g.db.SaveEpoch(ctx, g.cfg.Network.Name, epochResult, len(blobs)); err != nil {
		return cid.Cid{}, fmt.Errorf("save epoch %d to db: %w", epoch, err)
	}
	// db is guaranteed non-nil here (checked at the top of finalizeEpochInner)

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
	g.log.Info("uploading blocks to IPFS", "epoch", epoch, "blocks", total)
	progress := func(current, count int, blockCID string) {
		if current == count || current%10 == 0 {
			g.log.Info("IPFS upload progress", "epoch", epoch, "blocks", fmt.Sprintf("%d/%d", current, count), "cid", blockCID)
		}
	}
	if err := g.ipfs.PutBlockstore(ctx, bs, progress); err != nil {
		return fmt.Errorf("upload epoch %d to IPFS: %w", epoch, err)
	}
	g.log.Info("IPFS upload complete", "epoch", epoch, "total", total)
	if g.cfg.IPFS.PinOnAdd {
		if err := g.ipfs.Pin(ctx, epochCID); err != nil {
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

	rootResult, err := builder.BuildNetworkRoot(ctx, lsys, g.cfg.Network.Name, epochResults)
	if err != nil {
		return fmt.Errorf("build network root: %w", err)
	}

	if err := g.ipfs.PutBlockstore(ctx, rootBS); err != nil {
		return fmt.Errorf("upload network root to IPFS: %w", err)
	}

	g.log.Info("network root rebuilt", "cid", rootResult.CID, "epochs", len(epochResults))
	return nil
}

// processEpochBlobs processes all blobs in an epoch concurrently using a worker pool.
func (g *Generator) processEpochBlobs(ctx context.Context, epochInp types.EpochInput) (*store.MemBlockstore, []types.BlobResult, error) {
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

	epochBS := store.NewMemBlockstore()
	lsys := store.NewLinkSystem(epochBS)

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
			return nil, nil, r.err
		}
		blobResults[r.idx] = r.res
		g.log.Info("blob processed",
			"epoch", epochInp.Epoch,
			"slot", epochInp.Blobs[r.idx].Slot,
			"index", epochInp.Blobs[r.idx].Index,
			"commitment", epochInp.Blobs[r.idx].Commitment[:16]+"…",
			"data_cid", r.res.DataCID,
			"meta_cid", r.res.MetaCID,
		)
	}

	return epochBS, blobResults, nil
}

// ProcessSingleEpoch is a one-shot helper for backfilling or manual invocation.
func (g *Generator) ProcessSingleEpoch(ctx context.Context, epoch uint64) error {
	return g.processEpoch(ctx, epoch)
}

// hexDecode accepts 0x-prefixed or plain hex strings.
func hexDecode(s string) ([]byte, error) {
	s = strings.TrimPrefix(s, "0x")
	out := make([]byte, hex.DecodedLen(len(s)))
	n, err := hex.Decode(out, []byte(s))
	return out[:n], err
}
