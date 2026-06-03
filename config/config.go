package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config is the top-level configuration for the blobscan-ipld generator.
type Config struct {
	Network   NetworkConfig
	IPFS      IPFSConfig
	Storage   StorageConfig
	Generator GeneratorConfig
	Blobscan  BlobscanConfig
}

// BlobscanConfig holds connection settings for reporting CID references back to
// the blobscan REST API.
type BlobscanConfig struct {
	APIURL string // e.g. http://blobscan:3001
	APIKey string // value of IPFS_STORAGE_API_KEY in blobscan
}

// NetworkConfig identifies the Ethereum network being indexed.
type NetworkConfig struct {
	Name             string        // e.g. "mainnet", "sepolia"
	BeaconRPC        string        // Beacon node REST API base URL (optional when using the push API)
	BeaconTimeout    time.Duration // HTTP timeout for beacon requests (optional; default 60s)
	BeaconRateLimit  float64       // max requests per second to beacon RPC (optional; default 100)
	BeaconRateBurst  int           // token bucket burst size (optional; default 10)
	Beacon429Backoff time.Duration // initial backoff for 429 errors (optional; default 1s)
}

// IPFSConfig holds connection settings for the IPFS node.
type IPFSConfig struct {
	APIAddr       string        // e.g. "/ip4/127.0.0.1/tcp/5001"
	PinOnAdd      bool
	Timeout       time.Duration
	SkipUpload    bool // compute CIDs only; do not connect to or upload to IPFS
	UploadWorkers int  // parallel block uploads in PutBlockstore (default 16)
}

// StorageConfig controls local storage paths and the database connection.
type StorageConfig struct {
	DataDir     string // root directory for state file and CAR files
	CARDir      string // subdirectory for per-epoch CAR v2 files
	PostgresDSN string // PostgreSQL connection string (optional)
}

// GeneratorConfig controls the DAG generation behaviour.
type GeneratorConfig struct {
	HAMTThreshold       int           // blobs per epoch before switching to HAMT
	PollInterval        time.Duration // how often to check for new finalized epochs
	StartEpoch          uint64        // first epoch to process (0 = genesis)
	Workers             int           // parallel blob-processing goroutines
	BeaconWorkers       int           // parallel slot fetches per epoch
	SkipExistingEpochs  bool          // resume from last processed epoch
	APIListen           string        // address for the HTTP push API (e.g. ":8080"); empty = disabled
	NetworkRootPageSize int           // max epochs per NetworkRoot page (default 10000)
}

// Load builds a Config entirely from environment variables and built-in defaults.
func Load() (*Config, error) {
	cfg := &Config{}
	cfg.applyEnv()

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}

	cfg.applyDefaults()
	return cfg, nil
}

func (c *Config) applyEnv() {
	if v := os.Getenv("NETWORK_NAME"); v != "" {
		c.Network.Name = v
	}
	if v := os.Getenv("BEACON_RPC"); v != "" {
		c.Network.BeaconRPC = v
	}
	if v := os.Getenv("BEACON_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			c.Network.BeaconTimeout = d
		}
	}
	if v := os.Getenv("BEACON_RATE_LIMIT"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			c.Network.BeaconRateLimit = f
		}
	}
	if v := os.Getenv("BEACON_RATE_BURST"); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			c.Network.BeaconRateBurst = i
		}
	}
	if v := os.Getenv("BEACON_429_BACKOFF"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			c.Network.Beacon429Backoff = d
		}
	}
	if v := os.Getenv("POSTGRES_DSN"); v != "" {
		c.Storage.PostgresDSN = v
	}
	if v := os.Getenv("DATA_DIR"); v != "" {
		c.Storage.DataDir = v
	}
	if v := os.Getenv("CAR_DIR"); v != "" {
		c.Storage.CARDir = v
	}
	if v := os.Getenv("IPFS_API_ADDR"); v != "" {
		c.IPFS.APIAddr = v
	}
	if v := os.Getenv("IPFS_PIN_ON_ADD"); v == "true" || v == "1" {
		c.IPFS.PinOnAdd = true
	}
	if v := os.Getenv("IPFS_SKIP_UPLOAD"); v == "true" || v == "1" {
		c.IPFS.SkipUpload = true
	}
	if v := os.Getenv("IPFS_UPLOAD_WORKERS"); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			c.IPFS.UploadWorkers = i
		}
	}
	if v := os.Getenv("GENERATOR_WORKERS"); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			c.Generator.Workers = i
		}
	}
	if v := os.Getenv("GENERATOR_BEACON_WORKERS"); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			c.Generator.BeaconWorkers = i
		}
	}
	if v := os.Getenv("GENERATOR_POLL_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			c.Generator.PollInterval = d
		}
	}
	if v := os.Getenv("GENERATOR_START_EPOCH"); v != "" {
		if i, err := strconv.ParseUint(v, 10, 64); err == nil {
			c.Generator.StartEpoch = i
		}
	}
	if v := os.Getenv("GENERATOR_HAMT_THRESHOLD"); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			c.Generator.HAMTThreshold = i
		}
	}
	if v := os.Getenv("GENERATOR_SKIP_EXISTING_EPOCHS"); v == "true" || v == "1" {
		c.Generator.SkipExistingEpochs = true
	}
	if v := os.Getenv("GENERATOR_API_LISTEN"); v != "" {
		c.Generator.APIListen = v
	}
	if v := os.Getenv("NETWORK_ROOT_PAGE_SIZE"); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			c.Generator.NetworkRootPageSize = i
		}
	}
	if v := os.Getenv("BLOBSCAN_API_URL"); v != "" {
		c.Blobscan.APIURL = v
	}
	if v := os.Getenv("BLOBSCAN_API_KEY"); v != "" {
		c.Blobscan.APIKey = v
	}
}

func (c *Config) validate() error {
	if c.Network.Name == "" {
		return fmt.Errorf("NETWORK_NAME is required")
	}
	if c.IPFS.APIAddr == "" && !c.IPFS.SkipUpload {
		return fmt.Errorf("IPFS_API_ADDR is required (or set IPFS_SKIP_UPLOAD=true)")
	}
	if c.Storage.DataDir == "" {
		return fmt.Errorf("DATA_DIR is required")
	}
	if c.Generator.NetworkRootPageSize != 0 && c.Generator.NetworkRootPageSize < 1000 {
		return fmt.Errorf("NETWORK_ROOT_PAGE_SIZE must be >= 1000 (got %d)", c.Generator.NetworkRootPageSize)
	}
	return nil
}

// dencunEpoch returns the first epoch that has blob sidecars for known networks.
// Returns (0, false) for unknown networks.
func dencunEpoch(network string) (uint64, bool) {
	switch network {
	case "mainnet":
		return 269568, true
	case "sepolia":
		return 132608, true
	case "gnosis":
		return 889856, true
	case "hoodi":
		return 0, true
	default:
		return 0, false
	}
}

func (c *Config) applyDefaults() {
	if c.Generator.HAMTThreshold == 0 {
		c.Generator.HAMTThreshold = 5000
	}
	if c.Generator.PollInterval == 0 {
		c.Generator.PollInterval = 12 * time.Second
	}
	if c.Generator.Workers == 0 {
		c.Generator.Workers = 16
	}
	if c.Generator.BeaconWorkers == 0 {
		c.Generator.BeaconWorkers = 16
	}
	if c.Generator.NetworkRootPageSize == 0 {
		c.Generator.NetworkRootPageSize = 10000
	}
	if c.Storage.CARDir == "" {
		c.Storage.CARDir = c.Storage.DataDir + "/car"
	}
	if c.IPFS.Timeout == 0 {
		c.IPFS.Timeout = 30 * time.Second
	}
	if c.IPFS.UploadWorkers == 0 {
		c.IPFS.UploadWorkers = 16
	}
	if c.Network.BeaconTimeout == 0 {
		c.Network.BeaconTimeout = 60 * time.Second
	}
	if c.Network.BeaconRateLimit == 0 {
		c.Network.BeaconRateLimit = 100 // req/s
	}
	if c.Network.BeaconRateBurst == 0 {
		c.Network.BeaconRateBurst = 10
	}
	if c.Network.Beacon429Backoff == 0 {
		c.Network.Beacon429Backoff = 1 * time.Second
	}
	if c.Generator.StartEpoch == 0 {
		if epoch, ok := dencunEpoch(c.Network.Name); ok {
			c.Generator.StartEpoch = epoch
		}
	}
}
