// Package state handles persistence of generator progress between runs.
// State is stored as a JSON file in the data directory, or in PostgreSQL when
// a DB backend is available (see db.StateBackend).
package state

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/blobscan/blobscan-ipld/types"
)

// Backend is the minimal interface the generator needs for progress tracking.
// It is implemented by Manager (file-backed) and by db.Client (DB-backed).
type Backend interface {
	// GetLastProcessedEpoch returns the last fully processed epoch number.
	// Returns 0 and no error when no epoch has been processed yet.
	GetLastProcessedEpoch(ctx context.Context) (uint64, error)
	// SetLastProcessedEpoch persists the given epoch as the last processed.
	SetLastProcessedEpoch(ctx context.Context, epoch uint64) error
}

// Manager reads and writes generator state to a JSON file.
type Manager struct {
	mu      sync.RWMutex
	path    string
	current types.State
}

// NewManager creates a Manager backed by the given file path.
// If the file does not exist, an empty state is initialised.
func NewManager(dataDir, network string) (*Manager, error) {
	path := filepath.Join(dataDir, network+"-state.json")
	m := &Manager{path: path}

	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		m.current = types.State{Network: network}
		return m, nil
	}
	if err != nil {
		return nil, fmt.Errorf("state: read %q: %w", path, err)
	}

	if err := json.Unmarshal(data, &m.current); err != nil {
		return nil, fmt.Errorf("state: parse %q: %w", path, err)
	}
	return m, nil
}

// Get returns a copy of the current state.
func (m *Manager) Get() types.State {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.current
}

// GetLastProcessedEpoch implements Backend. Returns the last fully processed epoch.
func (m *Manager) GetLastProcessedEpoch(_ context.Context) (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.current.LastProcessedEpoch, nil
}

// SetLastProcessedEpoch implements Backend. Updates the epoch and persists atomically.
func (m *Manager) SetLastProcessedEpoch(_ context.Context, epoch uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.current.LastProcessedEpoch = epoch
	return m.save()
}

// save writes the current state to disk atomically.
func (m *Manager) save() error {
	data, err := json.MarshalIndent(m.current, "", "  ")
	if err != nil {
		return fmt.Errorf("state: marshal: %w", err)
	}

	tmp := m.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("state: write tmp %q: %w", tmp, err)
	}
	if err := os.Rename(tmp, m.path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("state: rename %q → %q: %w", tmp, m.path, err)
	}
	return nil
}
