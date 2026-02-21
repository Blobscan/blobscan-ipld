// Package store wraps a local blockstore used as a staging area before blocks
// are exported to CAR v2 or pushed to an IPFS node.
package store

import (
	"context"
	"errors"
	"sync"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

// ErrNotFound is returned by Get and GetSize when a block is absent.
var ErrNotFound = errors.New("blockstore: block not found")

// MemBlockstore is a thread-safe in-memory blockstore used for building a
// single range's DAG before exporting it to a CAR file.
type MemBlockstore struct {
	mu     sync.RWMutex
	blocks map[cid.Cid]blocks.Block
}

// NewMemBlockstore creates an empty in-memory blockstore.
func NewMemBlockstore() *MemBlockstore {
	return &MemBlockstore{
		blocks: make(map[cid.Cid]blocks.Block),
	}
}

// Put stores a block.
func (m *MemBlockstore) Put(_ context.Context, b blocks.Block) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.blocks[b.Cid()] = b
	return nil
}

// PutMany stores multiple blocks.
func (m *MemBlockstore) PutMany(_ context.Context, bs []blocks.Block) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, b := range bs {
		m.blocks[b.Cid()] = b
	}
	return nil
}

// Get retrieves a block by CID.
func (m *MemBlockstore) Get(_ context.Context, c cid.Cid) (blocks.Block, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	b, ok := m.blocks[c]
	if !ok {
		return nil, ErrNotFound
	}
	return b, nil
}

// Has reports whether the blockstore contains the given CID.
func (m *MemBlockstore) Has(_ context.Context, c cid.Cid) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.blocks[c]
	return ok, nil
}

// GetSize returns the size of the block data for the given CID.
func (m *MemBlockstore) GetSize(_ context.Context, c cid.Cid) (int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	b, ok := m.blocks[c]
	if !ok {
		return 0, ErrNotFound
	}
	return len(b.RawData()), nil
}

// DeleteBlock removes a block.
func (m *MemBlockstore) DeleteBlock(_ context.Context, c cid.Cid) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.blocks, c)
	return nil
}

// AllKeysChan returns a channel of all CIDs in the blockstore.
func (m *MemBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	m.mu.RLock()
	keys := make([]cid.Cid, 0, len(m.blocks))
	for c := range m.blocks {
		keys = append(keys, c)
	}
	m.mu.RUnlock()

	ch := make(chan cid.Cid, len(keys))
	go func() {
		defer close(ch)
		for _, c := range keys {
			select {
			case <-ctx.Done():
				return
			case ch <- c:
			}
		}
	}()
	return ch, nil
}

// HashOnRead is a no-op for the in-memory store.
func (m *MemBlockstore) HashOnRead(_ bool) {}

// Len returns the number of blocks currently stored.
func (m *MemBlockstore) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.blocks)
}

// All returns a snapshot of all blocks (for CAR export).
func (m *MemBlockstore) All() []blocks.Block {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]blocks.Block, 0, len(m.blocks))
	for _, b := range m.blocks {
		out = append(out, b)
	}
	return out
}


