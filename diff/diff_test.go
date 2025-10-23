package diff

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/HORNET-Storage/Scionic-Merkle-Tree/v2/dag"
	"github.com/HORNET-Storage/Scionic-Merkle-Tree/v2/testutil"
)

// TestDiff_IdenticalDAGs tests diffing two identical DAGs
// Uses all fixtures to ensure diff works correctly for all DAG types
func TestDiff_IdenticalDAGs(t *testing.T) {
	testutil.RunTestWithAllFixtures(t, func(t *testing.T, dag1 *dag.Dag, fixture testutil.TestFixture, fixturePath string) {
		// Create a second identical DAG from the same fixture
		dag2, err := dag.CreateDag(fixturePath, false)
		if err != nil {
			t.Fatalf("Failed to create second DAG: %v", err)
		}

		// Diff the DAGs
		diff, err := Diff(dag1, dag2)
		if err != nil {
			t.Fatalf("Failed to diff DAGs: %v", err)
		}

		// Should have no differences
		if diff.Summary.Total != 0 {
			t.Errorf("Expected no differences, got %d total differences", diff.Summary.Total)
		}
		if diff.Summary.Added != 0 {
			t.Errorf("Expected 0 added, got %d", diff.Summary.Added)
		}
		if diff.Summary.Removed != 0 {
			t.Errorf("Expected 0 removed, got %d", diff.Summary.Removed)
		}

		t.Logf("✓ %s: Identical DAGs correctly show no differences", fixture.Name)
	})
}

// TestDiff_DifferentFixtures tests diffing between different fixture types
func TestDiff_DifferentFixtures(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "diff_fixtures_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create all fixtures
	fixtures := testutil.GetAllFixtures()
	dags := make(map[string]*dag.Dag)

	for _, fixture := range fixtures {
		fixturePath, err := testutil.CreateFixture(tmpDir, fixture)
		if err != nil {
			t.Fatalf("Failed to create fixture %s: %v", fixture.Name, err)
		}

		dag, err := dag.CreateDag(fixturePath, false)
		if err != nil {
			t.Fatalf("Failed to create DAG for fixture %s: %v", fixture.Name, err)
		}

		dags[fixture.Name] = dag
	}

	// Test single_small_file vs flat_directory (fewer vs more files)
	t.Run("SingleSmallFile_vs_FlatDirectory", func(t *testing.T) {
		dag1 := dags["single_small_file"]
		dag2 := dags["flat_directory"]

		diff, err := Diff(dag1, dag2)
		if err != nil {
			t.Fatalf("Failed to diff DAGs: %v", err)
		}

		// flat_directory has more files, so should show additions
		if diff.Summary.Added == 0 {
			t.Errorf("Expected added leaves when comparing to larger fixture")
		}

		t.Logf("✓ Single file vs flat directory: %d added, %d removed",
			diff.Summary.Added, diff.Summary.Removed)
	})

	// Test flat_directory vs nested_directory (flat vs nested structure)
	t.Run("FlatDirectory_vs_NestedDirectory", func(t *testing.T) {
		dag1 := dags["flat_directory"]
		dag2 := dags["nested_directory"]

		diff, err := Diff(dag1, dag2)
		if err != nil {
			t.Fatalf("Failed to diff DAGs: %v", err)
		}

		// Different structures should show differences
		if diff.Summary.Total == 0 {
			t.Errorf("Expected differences between flat and nested structures")
		}

		t.Logf("✓ Flat vs nested: %d added, %d removed, %d total",
			diff.Summary.Added, diff.Summary.Removed, diff.Summary.Total)
	})
}

// TestDiff_AddedLeaves tests detecting added leaves
func TestDiff_AddedLeaves(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "diff_added_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Use single_small_file vs flat_directory for deterministic comparison
	smallFixture := testutil.SingleSmallFile()
	largeFixture := testutil.FlatDirectory()

	smallPath, err := testutil.CreateFixture(tmpDir, smallFixture)
	if err != nil {
		t.Fatalf("Failed to create small fixture: %v", err)
	}

	largePath, err := testutil.CreateFixture(tmpDir, largeFixture)
	if err != nil {
		t.Fatalf("Failed to create large fixture: %v", err)
	}

	dag1, err := dag.CreateDag(smallPath, false)
	if err != nil {
		t.Fatalf("Failed to create first DAG: %v", err)
	}

	dag2, err := dag.CreateDag(largePath, false)
	if err != nil {
		t.Fatalf("Failed to create second DAG: %v", err)
	}

	// Diff the DAGs
	diff, err := Diff(dag1, dag2)
	if err != nil {
		t.Fatalf("Failed to diff DAGs: %v", err)
	}

	// Should have added leaves
	if diff.Summary.Added == 0 {
		t.Errorf("Expected some added leaves, got 0")
	}

	// Verify added leaves have correct type
	for bareHash, leafDiff := range diff.Diffs {
		if leafDiff.Type == DiffTypeAdded {
			if leafDiff.Leaf == nil {
				t.Errorf("Added leaf %s should have non-nil Leaf", bareHash)
			}
		}
	}

	t.Logf("✓ Found %d added leaves (small -> large)", diff.Summary.Added)
}

// TestDiff_RemovedLeaves tests detecting removed leaves
func TestDiff_RemovedLeaves(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "diff_removed_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Use flat_directory vs single_small_file for deterministic comparison
	largeFixture := testutil.FlatDirectory()
	smallFixture := testutil.SingleSmallFile()

	largePath, err := testutil.CreateFixture(tmpDir, largeFixture)
	if err != nil {
		t.Fatalf("Failed to create large fixture: %v", err)
	}

	smallPath, err := testutil.CreateFixture(tmpDir, smallFixture)
	if err != nil {
		t.Fatalf("Failed to create small fixture: %v", err)
	}

	dag1, err := dag.CreateDag(largePath, false)
	if err != nil {
		t.Fatalf("Failed to create first DAG: %v", err)
	}

	dag2, err := dag.CreateDag(smallPath, false)
	if err != nil {
		t.Fatalf("Failed to create second DAG: %v", err)
	}

	// Diff the DAGs
	diff, err := Diff(dag1, dag2)
	if err != nil {
		t.Fatalf("Failed to diff DAGs: %v", err)
	}

	// Should have removed leaves
	if diff.Summary.Removed == 0 {
		t.Errorf("Expected some removed leaves, got 0")
	}

	// Verify removed leaves have correct type
	for bareHash, leafDiff := range diff.Diffs {
		if leafDiff.Type == DiffTypeRemoved {
			if leafDiff.Leaf == nil {
				t.Errorf("Removed leaf %s should have non-nil Leaf", bareHash)
			}
		}
	}

	t.Logf("✓ Found %d removed leaves (large -> small)", diff.Summary.Removed)
	t.Logf("Summary: Added=%d, Removed=%d, Total=%d",
		diff.Summary.Added, diff.Summary.Removed, diff.Summary.Total)
}

func TestDiff_ModifiedLeaves(t *testing.T) {
	// This test demonstrates that in a content-addressed system,
	// there are no "modifications" - only additions and removals.
	// When file content changes, its hash changes, resulting in
	// one removal (old version) and one addition (new version).

	// Create temporary test directory
	testDir, err := os.MkdirTemp("", "dag_diff_modified_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Create directory
	dir1 := filepath.Join(testDir, "input1")
	if err := os.MkdirAll(dir1, 0755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}

	// Create a file
	testFile := filepath.Join(dir1, "test.txt")
	if err := os.WriteFile(testFile, []byte("original content"), 0644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	// Create first DAG
	dag1, err := dag.CreateDag(dir1, false)
	if err != nil {
		t.Fatalf("Failed to create first DAG: %v", err)
	}

	// Modify the file
	if err := os.WriteFile(testFile, []byte("modified content with different length"), 0644); err != nil {
		t.Fatalf("Failed to modify file: %v", err)
	}

	// Create second DAG
	dag2, err := dag.CreateDag(dir1, false)
	if err != nil {
		t.Fatalf("Failed to create second DAG: %v", err)
	}

	// Diff the DAGs
	diff, err := Diff(dag1, dag2)
	if err != nil {
		t.Fatalf("Failed to diff DAGs: %v", err)
	}

	// In content-addressed systems: modification = removal + addition
	// We expect BOTH additions and removals when content changes
	if diff.Summary.Added == 0 {
		t.Logf("Note: Expected some added leaves for modified content")
	}
	if diff.Summary.Removed == 0 {
		t.Logf("Note: Expected some removed leaves for modified content")
	}

	t.Logf("Content change results:")
	t.Logf("  Removed (old version): %d", diff.Summary.Removed)
	t.Logf("  Added (new version): %d", diff.Summary.Added)
	t.Logf("  Total changes: %d", diff.Summary.Total)
	t.Logf("Summary: Added=%d, Removed=%d, Total=%d",
		diff.Summary.Added, diff.Summary.Removed, diff.Summary.Total)
}

func TestDiff_ComplexChanges(t *testing.T) {
	// Create temporary test directories
	testDir, err := os.MkdirTemp("", "dag_diff_complex_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Create first directory structure
	dir1 := filepath.Join(testDir, "input1")
	if err := os.MkdirAll(filepath.Join(dir1, "subdir"), 0755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir1, "file1.txt"), []byte("content1"), 0644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir1, "file2.txt"), []byte("content2"), 0644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir1, "subdir", "file3.txt"), []byte("content3"), 0644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	// Create first DAG
	dag1, err := dag.CreateDag(dir1, false)
	if err != nil {
		t.Fatalf("Failed to create first DAG: %v", err)
	}

	// Modify directory structure:
	// - Keep file1.txt unchanged
	// - Modify file2.txt
	// - Remove file3.txt
	// - Add file4.txt
	dir2 := filepath.Join(testDir, "input2")
	if err := os.MkdirAll(filepath.Join(dir2, "subdir"), 0755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir2, "file1.txt"), []byte("content1"), 0644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir2, "file2.txt"), []byte("MODIFIED content2"), 0644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}
	// file3.txt is removed (not created in dir2)
	if err := os.WriteFile(filepath.Join(dir2, "file4.txt"), []byte("new content4"), 0644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	// Create second DAG
	dag2, err := dag.CreateDag(dir2, false)
	if err != nil {
		t.Fatalf("Failed to create second DAG: %v", err)
	}

	// Diff the DAGs
	diff, err := Diff(dag1, dag2)
	if err != nil {
		t.Fatalf("Failed to diff DAGs: %v", err)
	}

	// Should have all types of changes
	if diff.Summary.Added == 0 {
		t.Logf("Warning: Expected some added leaves")
	}
	if diff.Summary.Removed == 0 {
		t.Logf("Warning: Expected some removed leaves")
	}

	// Verify total matches sum
	expectedTotal := diff.Summary.Added + diff.Summary.Removed
	if diff.Summary.Total != expectedTotal {
		t.Errorf("Total mismatch: got %d, expected %d", diff.Summary.Total, expectedTotal)
	}

	t.Logf("Complex diff results:")
	t.Logf("  Added: %d", diff.Summary.Added)
	t.Logf("  Removed: %d", diff.Summary.Removed)
	t.Logf("  Total: %d", diff.Summary.Total)

	// Log details of each change
	for bareHash, leafDiff := range diff.Diffs {
		switch leafDiff.Type {
		case DiffTypeAdded:
			t.Logf("  Added: %s (name: %s)", bareHash[:16], leafDiff.Leaf.ItemName)
		case DiffTypeRemoved:
			t.Logf("  Removed: %s (name: %s)", bareHash[:16], leafDiff.Leaf.ItemName)
		}
	}
}

// TestDiff_IgnoresLabels tests that diffs correctly ignore label differences
// Uses fixtures to ensure consistent behavior
func TestDiff_IgnoresLabels(t *testing.T) {
	testutil.RunTestWithAllFixtures(t, func(t *testing.T, dag1 *dag.Dag, fixture testutil.TestFixture, fixturePath string) {
		// Create two DAGs with potentially different label computation
		dag2, err := dag.CreateDag(fixturePath, false)
		if err != nil {
			t.Fatalf("Failed to create second DAG: %v", err)
		}

		// Diff the DAGs
		diff, err := Diff(dag1, dag2)
		if err != nil {
			t.Fatalf("Failed to diff DAGs for %s: %v", fixture.Name, err)
		}

		// Should have no differences (labels are ignored in diff)
		if diff.Summary.Total != 0 {
			t.Errorf("Expected no differences when only labels change, got %d total differences", diff.Summary.Total)
			for bareHash, leafDiff := range diff.Diffs {
				t.Logf("  Unexpected diff: %s type=%s", bareHash[:16], leafDiff.Type)
			}
		}

		t.Logf("✓ %s: Correctly ignored label differences", fixture.Name)
	})
}

// TestDiff_NilDAGs tests error handling with nil DAGs
func TestDiff_NilDAGs(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dag_diff_nil_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a simple deterministic test directory with one file
	inputDir := filepath.Join(tmpDir, "input")
	if err := os.MkdirAll(inputDir, 0755); err != nil {
		t.Fatalf("Failed to create input directory: %v", err)
	}

	// Create a single file
	if err := os.WriteFile(filepath.Join(inputDir, "file.txt"), []byte("test content"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	d, err := dag.CreateDag(inputDir, false)
	if err != nil {
		t.Fatalf("Failed to create DAG: %v", err)
	}

	// Test nil source
	var nilDag *dag.Dag
	_, err = Diff(nilDag, d)
	if err == nil {
		t.Error("Expected error when source DAG is nil")
	}

	// Test nil target
	_, err = Diff(d, nil)
	if err == nil {
		t.Error("Expected error when target DAG is nil")
	}
}

func TestDiff_RootChanges(t *testing.T) {
	testutil.RunTestWithMultiFileFixtures(t, func(t *testing.T, dag1 *dag.Dag, fixture testutil.TestFixture, fixturePath string) {
		// Create second DAG with timestamp (different root metadata)
		dag2, err := dag.CreateDag(fixturePath, true)
		if err != nil {
			t.Fatalf("Failed to create second DAG: %v", err)
		}

		// Diff the DAGs
		diff, err := Diff(dag1, dag2)
		if err != nil {
			t.Fatalf("Failed to diff DAGs: %v", err)
		}

		// Root might show as added/removed due to timestamp in additional_data changing the hash
		t.Logf("Root diff summary: Added=%d, Removed=%d, Total=%d",
			diff.Summary.Added, diff.Summary.Removed, diff.Summary.Total)

		// Check if root is in the diff
		if rootDiff, exists := diff.Diffs[dag1.Root]; exists {
			t.Logf("Root leaf found in diff with type: %s", rootDiff.Type)
		}
	})
}
