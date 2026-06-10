package beacon

import "testing"

// TestKZGCommitmentToVersionedHash guards against the placeholder bug where the
// versioned hash was computed as "0x01" + commitment[4:] instead of
// 0x01 || SHA256(commitment)[1:]. The expected value below is the real
// EIP-4844 versioned hash for a known mainnet blob (slot 8626178, index 0).
func TestKZGCommitmentToVersionedHash(t *testing.T) {
	commitment := "0xb28e4d255047f6e50b3d7548d37155b6e2289e82520aa6248d9fbe50e73b81d9f705cb3f2192d55caf54e26fb29c419a"
	want := "0x0175f564b393e44640ecffddce4010a42c1c966413987d8a59a253ef64a0c5cf"

	got, err := kzgCommitmentToVersionedHash(commitment)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != want {
		t.Errorf("versioned hash mismatch\n got: %s\nwant: %s", got, want)
	}
	if got[:4] != "0x01" {
		t.Errorf("versioned hash must start with version byte 0x01, got %s", got[:4])
	}
}

func TestKZGCommitmentToVersionedHashInvalidHex(t *testing.T) {
	if _, err := kzgCommitmentToVersionedHash("0xzz"); err == nil {
		t.Error("expected error for invalid hex, got nil")
	}
}
