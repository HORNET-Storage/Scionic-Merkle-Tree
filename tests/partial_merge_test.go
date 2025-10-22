package tests

import (
	"testing"

	"github.com/HORNET-Storage/Scionic-Merkle-Tree/dag"
	"github.com/HORNET-Storage/Scionic-Merkle-Tree/testutil"
)

// TestPartialDAGMergeBehavior explicitly tests that multiple branches are correctly merged
// This test verifies that when requesting multiple leaves from different parts of the tree,
// the partial DAG correctly merges their verification paths and proofs
func TestPartialDAGMergeBehavior(t *testing.T) {
	testutil.RunTestWithFixture(t, "nested_directory", func(t *testing.T, d *dag.Dag, fixturePath string) {
		// Verify full DAG
		err := d.Verify()
		if err != nil {
			t.Fatalf("Full DAG verification failed: %v", err)
		}

		t.Logf("Full DAG structure:")
		t.Logf("  Total leaves: %d", len(d.Leafs))

		// Find files from different subdirectories
		var filesFromSubdir1 []string
		var filesFromSubdir2 []string

		for hash, leaf := range d.Leafs {
			if leaf.Type == dag.FileLeafType {
				// Find parent
				for _, parent := range d.Leafs {
					if parent.HasLink(hash) && parent.Type == dag.DirectoryLeafType {
						if parent.ItemName == "subdir1" {
							filesFromSubdir1 = append(filesFromSubdir1, hash)
						} else if parent.ItemName == "subdir2" {
							filesFromSubdir2 = append(filesFromSubdir2, hash)
						}
						break
					}
				}
			}
		}

		t.Logf("  Files in subdir1: %d", len(filesFromSubdir1))
		t.Logf("  Files in subdir2: %d", len(filesFromSubdir2))

		if len(filesFromSubdir1) == 0 || len(filesFromSubdir2) == 0 {
			t.Skip("Need files in both subdirectories for merge test")
		}

		// Request one file from each subdirectory
		// This should build TWO separate branches that need to be merged
		requestedHashes := []string{filesFromSubdir1[0], filesFromSubdir2[0]}

		t.Logf("\nRequesting 2 files from different subdirectories...")
		partial, err := d.GetPartial(requestedHashes, true)
		if err != nil {
			t.Fatalf("Failed to create partial DAG: %v", err)
		}

		t.Logf("\nPartial DAG structure:")
		t.Logf("  Total leaves: %d", len(partial.Leafs))

		// Verify the partial DAG
		err = partial.Verify()
		if err != nil {
			t.Fatalf("Partial DAG verification failed: %v", err)
		}

		// The partial should include:
		// - 2 file leaves (requested)
		// - 2 directory leaves (parents)
		// - 1 root directory leaf
		// = 5 total leaves
		expectedLeaves := 5
		if len(partial.Leafs) != expectedLeaves {
			t.Logf("Warning: Expected %d leaves, got %d", expectedLeaves, len(partial.Leafs))
		}

		// Verify both requested files are in the partial
		for _, hash := range requestedHashes {
			if _, exists := partial.Leafs[hash]; !exists {
				t.Errorf("Requested file %s not found in partial DAG", hash)
			}
		}

		// Verify the root exists and has proofs for BOTH subdirectories
		rootLeaf := partial.Leafs[partial.Root]
		if rootLeaf == nil {
			t.Fatal("Root leaf not found in partial DAG")
		}

		t.Logf("\nRoot leaf:")
		t.Logf("  CurrentLinkCount: %d (original)", rootLeaf.CurrentLinkCount)
		t.Logf("  Actual Links: %d (pruned)", len(rootLeaf.Links))
		t.Logf("  Proofs: %d", len(rootLeaf.Proofs))

		// The root should have links to BOTH subdirectories (since we requested files from both)
		if len(rootLeaf.Links) < 2 {
			t.Errorf("Root should have links to both subdirectories, got %d links", len(rootLeaf.Links))
		}

		// Check each subdirectory has appropriate structure
		for dirName, expectedFiles := range map[string]int{"subdir1": 1, "subdir2": 1} {
			var dirLeaf *dag.DagLeaf
			for _, leaf := range partial.Leafs {
				if leaf.ItemName == dirName {
					dirLeaf = leaf
					break
				}
			}

			if dirLeaf == nil {
				t.Errorf("Directory %s not found in partial", dirName)
				continue
			}

			t.Logf("\n%s:", dirName)
			t.Logf("  CurrentLinkCount: %d (original)", dirLeaf.CurrentLinkCount)
			t.Logf("  Actual Links: %d (pruned)", len(dirLeaf.Links))
			t.Logf("  Proofs: %d", len(dirLeaf.Proofs))

			// If the directory originally had more than 1 file, it should have a proof for the one we kept
			if dirLeaf.CurrentLinkCount > 1 {
				if len(dirLeaf.Proofs) == 0 {
					t.Errorf("%s should have Merkle proofs for verification (has %d children but only %d links)",
						dirName, dirLeaf.CurrentLinkCount, len(dirLeaf.Links))
				}
			}

			if len(dirLeaf.Links) != expectedFiles {
				t.Errorf("%s should have %d file link, got %d", dirName, expectedFiles, len(dirLeaf.Links))
			}
		}

		t.Logf("\n✓ Successfully merged branches from different subdirectories!")
		t.Logf("✓ Partial DAG verified with complete proof structure!")
	})
}

// TestPartialDAGProofCompleteness verifies that all necessary proofs are included
func TestPartialDAGProofCompleteness(t *testing.T) {
	testutil.RunTestWithFixture(t, "flat_directory", func(t *testing.T, d *dag.Dag, fixturePath string) {
		// Get all file hashes
		var fileHashes []string
		for hash, leaf := range d.Leafs {
			if leaf.Type == dag.FileLeafType {
				fileHashes = append(fileHashes, hash)
			}
		}

		if len(fileHashes) < 3 {
			t.Skip("Need at least 3 files for proof completeness test")
		}

		// Request subset of files
		requestedHashes := fileHashes[:2]
		partial, err := d.GetPartial(requestedHashes, true)
		if err != nil {
			t.Fatalf("Failed to create partial DAG: %v", err)
		}

		// Get root from partial
		rootLeaf := partial.Leafs[partial.Root]
		if rootLeaf == nil {
			t.Fatal("Root not found in partial")
		}

		t.Logf("Root has %d children in original, %d links in partial, %d proofs",
			rootLeaf.CurrentLinkCount, len(rootLeaf.Links), len(rootLeaf.Proofs))

		// The root should have proofs for the files we kept
		for _, hash := range requestedHashes {
			if _, hasProof := rootLeaf.Proofs[hash]; !hasProof {
				t.Errorf("Root missing proof for requested file %s", hash)
			}
		}

		// Verify the partial
		err = partial.Verify()
		if err != nil {
			t.Errorf("Partial DAG verification failed: %v", err)
		}

		t.Logf("✓ All necessary proofs present and valid!")
	})
}
