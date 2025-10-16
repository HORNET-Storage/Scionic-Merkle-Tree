package dag

import (
	"os"
	"path/filepath"
	"testing"
)

// TestRecomputeLabelsDoesNotBreakVerification verifies that calling RecomputeLabels
// on a valid DAG does not break verification (since labels are not part of hash computation)
func TestRecomputeLabelsDoesNotBreakVerification(t *testing.T) {
	// Create a simple test directory
	tmpDir, err := os.MkdirTemp("", "recompute-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a simple file
	testFile := filepath.Join(tmpDir, "test.txt")
	if err := os.WriteFile(testFile, []byte("test content"), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	// Create DAG using standard method
	dag, err := CreateDag(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create DAG: %v", err)
	}

	// Verify DAG before RecomputeLabels
	if err := dag.Verify(); err != nil {
		t.Fatalf("DAG verification failed BEFORE RecomputeLabels: %v", err)
	}

	t.Logf("DAG verified successfully BEFORE RecomputeLabels")
	t.Logf("Root: %s", dag.Root)
	t.Logf("Leaf count: %d", len(dag.Leafs))
	for label, leaf := range dag.Leafs {
		t.Logf("  Leaf %s: hash=%s, type=%s", label, leaf.Hash, leaf.Type)
	}

	// Call RecomputeLabels
	if err := dag.RecomputeLabels(); err != nil {
		t.Fatalf("RecomputeLabels failed: %v", err)
	}

	t.Logf("\nDAG structure AFTER RecomputeLabels:")
	t.Logf("Root: %s", dag.Root)
	t.Logf("Leaf count: %d", len(dag.Leafs))
	for label, leaf := range dag.Leafs {
		t.Logf("  Leaf %s: hash=%s, type=%s", label, leaf.Hash, leaf.Type)
	}

	// Verify DAG after RecomputeLabels - THIS SHOULD STILL WORK
	if err := dag.Verify(); err != nil {
		// Print detailed error info
		t.Logf("\nDETAILED ERROR ANALYSIS:")
		for label, leaf := range dag.Leafs {
			t.Logf("Leaf map key: %s", label)
			t.Logf("  leaf.Hash field: %s", leaf.Hash)
			t.Logf("  Bare hash (GetHash): %s", GetHash(leaf.Hash))
			t.Logf("  Label (GetLabel): %s", GetLabel(leaf.Hash))

			// Try to verify this specific leaf
			leafErr := leaf.VerifyLeaf()
			if leafErr != nil {
				t.Logf("  ❌ Verification FAILED: %v", leafErr)
			} else {
				t.Logf("  ✓ Verification OK")
			}
		}

		t.Fatalf("DAG verification failed AFTER RecomputeLabels: %v\n"+
			"This is a BUG - labels should not affect hash verification!", err)
	}

	t.Logf("\nDAG verified successfully AFTER RecomputeLabels - labels do not affect hashes ✓")
}
