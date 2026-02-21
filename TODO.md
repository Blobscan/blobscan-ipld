# TODO / Ideas

## API Gateway

### Compressed blob downloads
Add an API gateway that serves blobs from IPFS with on-the-fly decompression
of trailing zero padding. EIP-4844 blobs are always padded to 128 KiB but not all
rollups use all available space, and fill the rest with zero bytes.
Storing and serving the raw 128 KiB wastes bandwidth and storage.

Ideas:
- Strip trailing zero bytes before storing the raw blob block (or store a
  separate "compressed" block alongside the raw one).
- Record the original unpadded length in `BlobMetadata` (new field:
  `unpaddedSize int`).
- Expose a REST endpoint `GET /blob/:commitment` that fetches the block from
  IPFS, trims the padding, and returns the useful payload — optionally with
  zstd/gzip `Content-Encoding` for further compression over the wire.
- Consider storing the unpadded blob as a separate IPLD raw node so the CID
  itself reflects the actual content (deduplicated across epochs if the same
  payload appears more than once).
