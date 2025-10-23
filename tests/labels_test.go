package tests

import (
	"fmt"
	"testing"

	"github.com/HORNET-Storage/Scionic-Merkle-Tree/v2/dag"
	"github.com/HORNET-Storage/Scionic-Merkle-Tree/v2/testutil"
)

// TestCalculateLabelsDeterminism verifies that CalculateLabels produces
// the same label assignments when called multiple times on the same DAG.
func TestCalculateLabelsDeterminism(t *testing.T) {
	fixtures := []struct {
		name    string
		fixture testutil.TestFixture
	}{
		{"SingleSmallFile", testutil.SingleSmallFile()},
		{"SingleLargeFile", testutil.SingleLargeFile()},
		{"FlatDirectory", testutil.FlatDirectory()},
		{"NestedDirectory", testutil.NestedDirectory()},
		{"DeepHierarchy", testutil.DeepHierarchy()},
		{"MixedSizes", testutil.MixedSizes()},
	}

	for _, tc := range fixtures {
		t.Run(tc.name, func(t *testing.T) {
			// Create temp directory
			tempDir := t.TempDir()

			// Setup fixture
			if err := tc.fixture.Setup(tempDir); err != nil {
				t.Fatalf("Failed to setup fixture: %v", err)
			}

			// Build DAG
			testDag, err := dag.CreateDag(tempDir, false)
			if err != nil {
				t.Fatalf("Failed to create DAG: %v", err)
			}

			// Calculate labels multiple times and verify consistency
			const iterations = 5
			var labelSnapshots []map[string]string

			for i := 0; i < iterations; i++ {
				// Calculate labels
				err := testDag.CalculateLabels()
				if err != nil {
					t.Fatalf("Iteration %d: CalculateLabels failed: %v", i, err)
				}

				// Take a snapshot of the current labels
				snapshot := make(map[string]string)
				for label, hash := range testDag.Labels {
					snapshot[label] = hash
				}
				labelSnapshots = append(labelSnapshots, snapshot)

				// Verify labels map is not empty (except for single leaf DAGs where root is the only leaf)
				if len(testDag.Leafs) > 1 && len(testDag.Labels) == 0 {
					t.Errorf("Iteration %d: Labels map is empty but DAG has %d leaves", i, len(testDag.Leafs))
				}

				// Verify root is not in the labels map
				for label, hash := range testDag.Labels {
					if hash == testDag.Root {
						t.Errorf("Iteration %d: Root hash found in labels map with label %s", i, label)
					}
				}
			}

			// Compare all snapshots to verify they're identical
			firstSnapshot := labelSnapshots[0]
			for i := 1; i < iterations; i++ {
				snapshot := labelSnapshots[i]

				// Check if maps have the same size
				if len(snapshot) != len(firstSnapshot) {
					t.Errorf("Iteration %d: Label count mismatch. Expected %d, got %d",
						i, len(firstSnapshot), len(snapshot))
					continue
				}

				// Check if all labels map to the same hashes
				for label, hash := range firstSnapshot {
					if snapshot[label] != hash {
						t.Errorf("Iteration %d: Label %s mismatch. Expected hash %s, got %s",
							i, label, hash, snapshot[label])
					}
				}
			}

			t.Logf("✓ DAG with %d leaves produced consistent labels across %d iterations", len(testDag.Leafs), iterations)
		})
	}
}

// TestClearLabels verifies that ClearLabels properly removes all label assignments.
func TestClearLabels(t *testing.T) {
	// Create temp directory
	tempDir := t.TempDir()

	// Use a fixture with multiple leaves
	fixture := testutil.NestedDirectory()
	if err := fixture.Setup(tempDir); err != nil {
		t.Fatalf("Failed to setup fixture: %v", err)
	}

	// Build DAG
	testDag, err := dag.CreateDag(tempDir, false)
	if err != nil {
		t.Fatalf("Failed to create DAG: %v", err)
	}

	// Calculate labels
	err = testDag.CalculateLabels()
	if err != nil {
		t.Fatalf("CalculateLabels failed: %v", err)
	}

	// Verify labels exist
	if len(testDag.Labels) == 0 {
		t.Fatal("Labels map is empty after CalculateLabels")
	}
	initialLabelCount := len(testDag.Labels)
	t.Logf("Initial label count: %d", initialLabelCount)

	// Clear labels
	testDag.ClearLabels()

	// Verify labels are cleared
	if len(testDag.Labels) != 0 {
		t.Errorf("Labels map not empty after ClearLabels. Contains %d labels", len(testDag.Labels))
	}

	// Verify we can recalculate labels after clearing
	err = testDag.CalculateLabels()
	if err != nil {
		t.Fatalf("CalculateLabels failed after ClearLabels: %v", err)
	}

	if len(testDag.Labels) != initialLabelCount {
		t.Errorf("Label count after recalculation mismatch. Expected %d, got %d",
			initialLabelCount, len(testDag.Labels))
	}

	t.Logf("✓ ClearLabels successfully cleared %d labels and recalculation works", initialLabelCount)
}

// TestLabelTraversalOrder verifies that labels follow the DAG traversal order.
func TestLabelTraversalOrder(t *testing.T) {
	// Create temp directory
	tempDir := t.TempDir()

	// Use a fixture with known structure
	fixture := testutil.NestedDirectory()
	if err := fixture.Setup(tempDir); err != nil {
		t.Fatalf("Failed to setup fixture: %v", err)
	}

	// Build DAG
	testDag, err := dag.CreateDag(tempDir, false)
	if err != nil {
		t.Fatalf("Failed to create DAG: %v", err)
	}

	// Calculate labels
	err = testDag.CalculateLabels()
	if err != nil {
		t.Fatalf("CalculateLabels failed: %v", err)
	}

	// Manually iterate through the DAG and collect hashes in traversal order
	var traversalOrder []string
	err = testDag.IterateDag(func(leaf *dag.DagLeaf, parent *dag.DagLeaf) error {
		// Skip root
		if leaf.Hash != testDag.Root {
			traversalOrder = append(traversalOrder, leaf.Hash)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("IterateDag failed: %v", err)
	}

	// Verify that each hash in traversal order appears in the labels
	for i, hash := range traversalOrder {
		found := false
		for _, labelHash := range testDag.Labels {
			if labelHash == hash {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Hash at traversal position %d not found in labels: %s", i, hash)
		}
	}

	// Verify the count matches
	if len(traversalOrder) != len(testDag.Labels) {
		t.Errorf("Traversal order count (%d) doesn't match labels count (%d)",
			len(traversalOrder), len(testDag.Labels))
	}

	// Verify labels are sequential from "1" to len(traversalOrder)
	for i := 1; i <= len(testDag.Labels); i++ {
		labelStr := fmt.Sprintf("%d", i)
		if _, exists := testDag.Labels[labelStr]; !exists {
			t.Errorf("Expected label %s not found in labels map", labelStr)
		}
	}

	t.Logf("✓ Labels follow DAG traversal order with %d labeled leaves", len(testDag.Labels))
}

// TestGetHashesByLabelRange verifies that GetHashesByLabelRange returns the correct hashes.
func TestGetHashesByLabelRange(t *testing.T) {
	// Create temp directory
	tempDir := t.TempDir()

	// Use a fixture with multiple leaves
	fixture := testutil.DeepHierarchy()
	if err := fixture.Setup(tempDir); err != nil {
		t.Fatalf("Failed to setup fixture: %v", err)
	}

	// Build DAG
	testDag, err := dag.CreateDag(tempDir, false)
	if err != nil {
		t.Fatalf("Failed to create DAG: %v", err)
	}

	// Test error when labels not calculated
	_, err = testDag.GetHashesByLabelRange("1", "5")
	if err == nil {
		t.Error("Expected error when labels not calculated, got nil")
	}

	// Calculate labels
	err = testDag.CalculateLabels()
	if err != nil {
		t.Fatalf("CalculateLabels failed: %v", err)
	}

	totalLabels := len(testDag.Labels)
	t.Logf("Total labels: %d", totalLabels)

	// Test valid range
	t.Run("ValidRange", func(t *testing.T) {
		hashes, err := testDag.GetHashesByLabelRange("1", "3")
		if err != nil {
			t.Fatalf("GetHashesByLabelRange failed: %v", err)
		}

		if len(hashes) != 3 {
			t.Errorf("Expected 3 hashes, got %d", len(hashes))
		}

		// Verify the hashes match the labels
		for i, hash := range hashes {
			label := fmt.Sprintf("%d", i+1)
			expectedHash := testDag.Labels[label]
			if hash != expectedHash {
				t.Errorf("Hash at index %d mismatch. Expected %s, got %s", i, expectedHash, hash)
			}
		}
	})

	// Test single label
	t.Run("SingleLabel", func(t *testing.T) {
		hashes, err := testDag.GetHashesByLabelRange("5", "5")
		if err != nil {
			t.Fatalf("GetHashesByLabelRange failed: %v", err)
		}

		if len(hashes) != 1 {
			t.Errorf("Expected 1 hash, got %d", len(hashes))
		}

		if hashes[0] != testDag.Labels["5"] {
			t.Errorf("Hash mismatch. Expected %s, got %s", testDag.Labels["5"], hashes[0])
		}
	})

	// Test full range
	t.Run("FullRange", func(t *testing.T) {
		endLabel := fmt.Sprintf("%d", totalLabels)
		hashes, err := testDag.GetHashesByLabelRange("1", endLabel)
		if err != nil {
			t.Fatalf("GetHashesByLabelRange failed: %v", err)
		}

		if len(hashes) != totalLabels {
			t.Errorf("Expected %d hashes, got %d", totalLabels, len(hashes))
		}

		// Verify all hashes are present
		for i := 1; i <= totalLabels; i++ {
			label := fmt.Sprintf("%d", i)
			expectedHash := testDag.Labels[label]
			found := false
			for _, hash := range hashes {
				if hash == expectedHash {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Hash for label %s not found in range", label)
			}
		}
	})

	// Test invalid ranges
	t.Run("InvalidRanges", func(t *testing.T) {
		tests := []struct {
			name       string
			startLabel string
			endLabel   string
		}{
			{"StartLessThanOne", "0", "5"},
			{"EndLessThanStart", "5", "3"},
			{"EndExceedsTotal", "1", fmt.Sprintf("%d", totalLabels+10)},
			{"InvalidStartFormat", "abc", "5"},
			{"InvalidEndFormat", "1", "xyz"},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				_, err := testDag.GetHashesByLabelRange(tc.startLabel, tc.endLabel)
				if err == nil {
					t.Errorf("Expected error for range %s-%s, got nil", tc.startLabel, tc.endLabel)
				}
				t.Logf("Got expected error: %v", err)
			})
		}
	})

	t.Logf("✓ GetHashesByLabelRange works correctly")
}

// TestGetLabel verifies that GetLabel returns the correct label for a given hash.
func TestGetLabel(t *testing.T) {
	// Create temp directory
	tempDir := t.TempDir()

	// Use a fixture with multiple leaves
	fixture := testutil.NestedDirectory()
	if err := fixture.Setup(tempDir); err != nil {
		t.Fatalf("Failed to setup fixture: %v", err)
	}

	// Build DAG
	testDag, err := dag.CreateDag(tempDir, false)
	if err != nil {
		t.Fatalf("Failed to create DAG: %v", err)
	}

	// Test error when labels not calculated
	_, err = testDag.GetLabel(testDag.Root)
	if err != nil {
		t.Errorf("GetLabel for root should work even without calculated labels, got error: %v", err)
	}

	// For non-root hashes, should get error
	var someHash string
	for hash := range testDag.Leafs {
		if hash != testDag.Root {
			someHash = hash
			break
		}
	}
	_, err = testDag.GetLabel(someHash)
	if err == nil {
		t.Error("Expected error when labels not calculated for non-root hash, got nil")
	}

	// Calculate labels
	err = testDag.CalculateLabels()
	if err != nil {
		t.Fatalf("CalculateLabels failed: %v", err)
	}

	t.Logf("Total labels: %d", len(testDag.Labels))

	// Test root hash
	t.Run("RootHash", func(t *testing.T) {
		label, err := testDag.GetLabel(testDag.Root)
		if err != nil {
			t.Fatalf("GetLabel failed for root: %v", err)
		}

		if label != "0" {
			t.Errorf("Expected label '0' for root, got %q", label)
		}
	})

	// Test all labeled hashes
	t.Run("AllLabels", func(t *testing.T) {
		for expectedLabel, hash := range testDag.Labels {
			label, err := testDag.GetLabel(hash)
			if err != nil {
				t.Errorf("GetLabel failed for hash %s: %v", hash, err)
			}

			if label != expectedLabel {
				t.Errorf("Label mismatch for hash %s. Expected %q, got %q", hash, expectedLabel, label)
			}
		}
	})

	// Test invalid hash
	t.Run("InvalidHash", func(t *testing.T) {
		_, err := testDag.GetLabel("invalid_hash_12345")
		if err == nil {
			t.Error("Expected error for invalid hash, got nil")
		}
		t.Logf("Got expected error: %v", err)
	})

	// Test round-trip: label -> hash -> label
	t.Run("RoundTrip", func(t *testing.T) {
		for originalLabel, hash := range testDag.Labels {
			retrievedLabel, err := testDag.GetLabel(hash)
			if err != nil {
				t.Fatalf("GetLabel failed: %v", err)
			}

			if retrievedLabel != originalLabel {
				t.Errorf("Round-trip failed. Original label %q, retrieved label %q", originalLabel, retrievedLabel)
			}
		}
	})

	t.Logf("✓ GetLabel works correctly")
}
