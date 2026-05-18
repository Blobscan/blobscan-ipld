// Package blobscan provides an HTTP client for registering IPFS CID references
// with the blobscan REST API after blobs are stored on IPFS.
package blobscan

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// BlobReference holds the versioned hash and its IPFS CIDs.
type BlobReference struct {
	VersionedHash string `json:"versionedHash"`
	DataCID       string `json:"dataCid"`
	MetaCID       string `json:"metaCid"`
}

// Notifier sends CID references to the blobscan REST API.
type Notifier struct {
	apiURL string
	apiKey string
	client *http.Client
	log    *slog.Logger
}

// NewNotifier creates a Notifier. Returns nil when apiURL is empty (disabled).
func NewNotifier(apiURL, apiKey string, log *slog.Logger) *Notifier {
	if apiURL == "" {
		return nil
	}
	return &Notifier{
		apiURL: strings.TrimRight(apiURL, "/"),
		apiKey: apiKey,
		client: &http.Client{Timeout: 30 * time.Second},
		log:    log,
	}
}

// NotifyBlobs sends a batch of blob CID references to blobscan. Errors are
// logged but never propagated — IPFS CID registration is non-fatal.
func (n *Notifier) NotifyBlobs(ctx context.Context, refs []BlobReference) {
	if n == nil || len(refs) == 0 {
		return
	}

	body, err := json.Marshal(map[string]any{"references": refs})
	if err != nil {
		n.log.Warn("blobscan notifier: failed to marshal references", "err", err)
		return
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		n.apiURL+"/blobs/ipfs-references",
		bytes.NewReader(body),
	)
	if err != nil {
		n.log.Warn("blobscan notifier: failed to build request", "err", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if n.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+n.apiKey)
	}

	resp, err := n.client.Do(req)
	if err != nil {
		n.log.Warn("blobscan notifier: request failed", "err", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		n.log.Warn("blobscan notifier: unexpected response",
			"status", resp.StatusCode,
			"blobs", len(refs),
			"err", fmt.Sprintf("HTTP %d", resp.StatusCode),
		)
		return
	}

	n.log.Debug("blobscan notifier: CID references registered", "blobs", len(refs))
}
