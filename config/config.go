package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the top-level configuration for the blobscan-ipld generator.
type Config struct {
	Network   NetworkConfig   `yaml:"network"`
	IPFS      IPFSConfig      `yaml:"ipfs"`
	Storage   StorageConfig   `yaml:"storage"`
	Generator GeneratorConfig `yaml:"generator"`
	Blobscan  BlobscanConfig  `yaml:"blobscan"`
}

// BlobscanConfig holds connection settings for reporting CID references back to
// the blobscan REST API.
type BlobscanConfig struct {
	APIURL string `yaml:"api_url"` // e.g. http://blobscan:3001
	APIKey string `yaml:"api_key"` // value of IPFS_STORAGE_API_KEY in blobscan
}

// NetworkConfig identifies the Ethereum network being indexed.
type NetworkConfig struct {
	Name              string        `yaml:"name"`                // e.g. "mainnet", "sepolia"
	BeaconRPC         string        `yaml:"beacon_rpc"`          // Beacon node REST API base URL (optional when using the push API)
	BeaconTimeout     time.Duration `yaml:"beacon_timeout"`      // HTTP timeout for beacon requests (optional; default 60s)
	BeaconRateLimit   float64       `yaml:"beacon_rate_limit"`   // max requests per second to beacon RPC (optional; default 100)
	BeaconRateBurst   int           `yaml:"beacon_rate_burst"`   // token bucket burst size (optional; default 10)
	Beacon429Backoff  time.Duration `yaml:"beacon_429_backoff"`  // initial backoff for 429 errors (optional; default 1s)
}

// IPFSConfig holds connection settings for the IPFS node.
type IPFSConfig struct {
	APIAddr       string        `yaml:"api_addr"`       // e.g. "/ip4/127.0.0.1/tcp/5001"
	PinOnAdd      bool          `yaml:"pin_on_add"`
	Timeout       time.Duration `yaml:"timeout"`
	SkipUpload    bool          `yaml:"skip_upload"`    // compute CIDs only; do not connect to or upload to IPFS
	UploadWorkers int           `yaml:"upload_workers"` // parallel block uploads in PutBlockstore (default 16)
}

// StorageConfig controls local storage paths and the database connection.
type StorageConfig struct {
	DataDir     string `yaml:"data_dir"`     // root directory for state file and CAR files
	CARDir      string `yaml:"car_dir"`      // subdirectory for per-epoch CAR v2 files
	PostgresDSN string `yaml:"postgres_dsn"` // PostgreSQL connection string (optional)
}

// GeneratorConfig controls the DAG generation behaviour.
type GeneratorConfig struct {
	HAMTThreshold       int           `yaml:"hamt_threshold"`        // blobs per epoch before switching to HAMT
	PollInterval        time.Duration `yaml:"poll_interval"`         // how often to check for new finalized epochs
	StartEpoch          uint64        `yaml:"start_epoch"`           // first epoch to process (0 = genesis)
	Workers             int           `yaml:"workers"`               // parallel blob-processing goroutines
	BeaconWorkers       int           `yaml:"beacon_workers"`        // parallel slot fetches per epoch
	SkipExistingEpochs  bool          `yaml:"skip_existing_epochs"`  // resume from last processed epoch
	APIListen           string        `yaml:"api_listen"`            // address for the HTTP push API (e.g. ":8080"); empty = disabled
	NetworkRootPageSize int           `yaml:"network_root_page_size"` // max epochs per NetworkRoot page (default 10000)
}

// Load reads and validates a YAML config file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read file %q: %w", path, err)
	}

	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("config: parse %q: %w", path, err)
	}

	cfg.applyEnvOverrides()

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("config: validation: %w", err)
	}

	cfg.applyDefaults()
	return cfg, nil
}

func (c *Config) applyEnvOverrides() {
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
	if v := os.Getenv("IPFS_API_ADDR"); v != "" {
		c.IPFS.APIAddr = v
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
		return fmt.Errorf("network.name is required")
	}
	if c.IPFS.APIAddr == "" && !c.IPFS.SkipUpload {
		return fmt.Errorf("ipfs.api_addr is required (or set ipfs.skip_upload: true)")
	}
	if c.Storage.DataDir == "" {
		return fmt.Errorf("storage.data_dir is required")
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
	if c.Generator.NetworkRootPageSize < 1000 {
		c.Generator.NetworkRootPageSize = 1000
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
