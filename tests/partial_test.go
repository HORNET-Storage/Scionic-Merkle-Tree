package tests

import (
	"testing"

	"github.com/HORNET-Storage/Scionic-Merkle-Tree/dag"
	"github.com/HORNET-Storage/Scionic-Merkle-Tree/testutil"
)

// TestGetPartial tests creating partial DAGs from full DAGs with multiple files
// Uses multi-file fixtures since you need multiple files to create a meaningful partial
func TestGetPartial(t *testing.T) {
	testutil.RunTestWithMultiFileFixtures(t, func(t *testing.T, d *dag.Dag, fixture testutil.TestFixture, fixturePath string) {
		// Verify the DAG was created correctly
		err := d.Verify()
		if err != nil {
			t.Fatalf("DAG verification failed: %v", err)
		}

		// Collect file leaf hashes
		var fileHashes []string
		for hash, leaf := range d.Leafs {
			if leaf.Type == dag.FileLeafType {
				fileHashes = append(fileHashes, hash)
			}
		}

		if len(fileHashes) < 2 {
			t.Skipf("Fixture %s doesn't have enough files for partial test", fixture.Name)
		}

		// Test getting a partial DAG with half the files (at least 1, at most all-1)
		partialCount := len(fileHashes) / 2
		if partialCount < 1 {
			partialCount = 1
		}
		if partialCount >= len(fileHashes) {
			partialCount = len(fileHashes) - 1
		}

		partialHashes := fileHashes[:partialCount]
		partial, err := d.GetPartial(partialHashes, true)
		if err != nil {
			t.Fatalf("Failed to get partial DAG: %v", err)
		}

		// Verify the partial DAG
		err = partial.Verify()
		if err != nil {
			t.Errorf("Partial DAG verification failed: %v", err)
		}

		// Verify partial DAG is actually partial
		if !partial.IsPartial() {
			t.Error("Partial DAG should be marked as partial")
		}

		t.Logf("Created partial DAG with %d/%d files from %s", partialCount, len(fileHashes), fixture.Name)
	})

	// Test error cases with a simple fixture
	t.Run("error_cases", func(t *testing.T) {
		testutil.RunTestWithFixture(t, "flat_directory", func(t *testing.T, dag *dag.Dag, fixturePath string) {
			// Test invalid hash
			_, err := dag.GetPartial([]string{"invalid_hash_that_doesnt_exist"}, true)
			if err == nil {
				t.Error("GetPartial should return error for invalid hash")
			}

			// Test empty array
			_, err = dag.GetPartial([]string{}, true)
			if err == nil {
				t.Error("GetPartial should return error for empty hash array")
			}
		})
	})
}

// TestGetPartialWithSingleFile tests that GetPartial works even with single file
// (though the result isn't really "partial" - it's the whole DAG)
func TestGetPartialWithSingleFile(t *testing.T) {
	// Use single file fixtures to test edge case
	t.Run("single_small_file", func(t *testing.T) {
		testutil.RunTestWithFixture(t, "single_small_file", func(t *testing.T, d *dag.Dag, fixturePath string) {
			// Verify the DAG was created correctly
			err := d.Verify()
			if err != nil {
				t.Fatalf("DAG verification failed: %v", err)
			}

			// Get the file leaf hash
			var fileHash string
			for hash, leaf := range d.Leafs {
				if leaf.Type == dag.FileLeafType {
					fileHash = hash
					break
				}
			}
			if fileHash == "" {
				t.Fatal("No file leaves found in the DAG")
			}

			// Test getting "partial" DAG with the single file (not really partial)
			partial, err := d.GetPartial([]string{fileHash}, true)
			if err != nil {
				t.Fatalf("Failed to get partial DAG: %v", err)
			}

			// Verify the "partial" DAG
			err = partial.Verify()
			if err != nil {
				t.Errorf("Partial DAG verification failed: %v", err)
			}

			// Note: With only one file, the "partial" is actually the complete DAG
			// This is an edge case but should still work
			t.Logf("Single file 'partial' DAG has %d leaves (expected to match full DAG)", len(partial.Leafs))
		})
	})
}

// TestGetPartialFromNestedStructure tests partial DAG creation from nested directories
// This is important because the verification path needs to traverse multiple levels
func TestGetPartialFromNestedStructure(t *testing.T) {
	// Use hierarchy fixtures specifically
	testutil.RunTestWithFixture(t, "nested_directory", func(t *testing.T, d *dag.Dag, fixturePath string) {
		// Verify the DAG was created correctly
		err := d.Verify()
		if err != nil {
			t.Fatalf("DAG verification failed: %v", err)
		}

		// Get file leaf hashes from different subdirectories
		var fileHash string
		for hash, leaf := range d.Leafs {
			if leaf.Type == dag.FileLeafType {
				fileHash = hash
				break
			}
		}
		if fileHash == "" {
			t.Fatal("No file leaves found in the DAG")
		}

		// Test getting partial DAG with one file from a subdirectory
		partial, err := d.GetPartial([]string{fileHash}, true)
		if err != nil {
			t.Fatalf("Failed to get partial DAG: %v", err)
		}

		// Verify the partial DAG
		err = partial.Verify()
		if err != nil {
			t.Errorf("Partial DAG verification failed: %v", err)
		}

		// The partial should include the file, its parent directory, and the root
		t.Logf("Nested partial DAG has %d leaves (includes verification path)", len(partial.Leafs))
	})

	// Test with deep hierarchy
	t.Run("deep_hierarchy", func(t *testing.T) {
		testutil.RunTestWithFixture(t, "deep_hierarchy", func(t *testing.T, d *dag.Dag, fixturePath string) {
			err := d.Verify()
			if err != nil {
				t.Fatalf("DAG verification failed: %v", err)
			}

			// Get a file from deep in the hierarchy
			var deepFileHash string
			for hash, leaf := range d.Leafs {
				if leaf.Type == dag.FileLeafType {
					deepFileHash = hash
					break
				}
			}

			if deepFileHash == "" {
				t.Skip("No file leaves found")
			}

			partial, err := d.GetPartial([]string{deepFileHash}, true)
			if err != nil {
				t.Fatalf("Failed to get partial DAG from deep hierarchy: %v", err)
			}

			err = partial.Verify()
			if err != nil {
				t.Errorf("Deep partial DAG verification failed: %v", err)
			}

			t.Logf("Deep hierarchy partial has %d leaves (full dag has %d)", len(partial.Leafs), len(d.Leafs))
		})
	})
}
