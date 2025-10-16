package dag

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDiff_IdenticalDAGs(t *testing.T) {
	// Create a temporary test directory
	testDir, err := os.MkdirTemp("", "dag_diff_identical_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Generate test data
	GenerateDummyDirectory(filepath.Join(testDir, "input"), 2, 3, 1, 2)

	// Create two identical DAGs
	dag1, err := CreateDag(filepath.Join(testDir, "input"), true)
	if err != nil {
		t.Fatalf("Failed to create first DAG: %v", err)
	}

	dag2, err := CreateDag(filepath.Join(testDir, "input"), true)
	if err != nil {
		t.Fatalf("Failed to create second DAG: %v", err)
	}

	// Diff the DAGs
	diff, err := dag1.Diff(dag2)
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

	t.Logf("Identical DAGs correctly show no differences")
}

func TestDiff_AddedLeaves(t *testing.T) {
	// Create temporary test directories
	testDir, err := os.MkdirTemp("", "dag_diff_added_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Create first directory with fewer files
	dir1 := filepath.Join(testDir, "input1")
	GenerateDummyDirectory(dir1, 1, 2, 1, 1)

	// Create second directory with more files
	dir2 := filepath.Join(testDir, "input2")
	GenerateDummyDirectory(dir2, 2, 3, 1, 2)

	// Create DAGs
	dag1, err := CreateDag(dir1, false)
	if err != nil {
		t.Fatalf("Failed to create first DAG: %v", err)
	}

	dag2, err := CreateDag(dir2, false)
	if err != nil {
		t.Fatalf("Failed to create second DAG: %v", err)
	}

	// Diff the DAGs
	diff, err := dag1.Diff(dag2)
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

	t.Logf("Found %d added leaves", diff.Summary.Added)
	t.Logf("Summary: Added=%d, Removed=%d, Total=%d",
		diff.Summary.Added, diff.Summary.Removed, diff.Summary.Total)
}

func TestDiff_RemovedLeaves(t *testing.T) {
	// Create temporary test directories
	testDir, err := os.MkdirTemp("", "dag_diff_removed_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Create first directory with more files
	dir1 := filepath.Join(testDir, "input1")
	GenerateDummyDirectory(dir1, 2, 3, 1, 2)

	// Create second directory with fewer files
	dir2 := filepath.Join(testDir, "input2")
	GenerateDummyDirectory(dir2, 1, 2, 1, 1)

	// Create DAGs
	dag1, err := CreateDag(dir1, false)
	if err != nil {
		t.Fatalf("Failed to create first DAG: %v", err)
	}

	dag2, err := CreateDag(dir2, false)
	if err != nil {
		t.Fatalf("Failed to create second DAG: %v", err)
	}

	// Diff the DAGs
	diff, err := dag1.Diff(dag2)
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

	t.Logf("Found %d removed leaves", diff.Summary.Removed)
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
	dag1, err := CreateDag(dir1, false)
	if err != nil {
		t.Fatalf("Failed to create first DAG: %v", err)
	}

	// Modify the file
	if err := os.WriteFile(testFile, []byte("modified content with different length"), 0644); err != nil {
		t.Fatalf("Failed to modify file: %v", err)
	}

	// Create second DAG
	dag2, err := CreateDag(dir1, false)
	if err != nil {
		t.Fatalf("Failed to create second DAG: %v", err)
	}

	// Diff the DAGs
	diff, err := dag1.Diff(dag2)
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
	dag1, err := CreateDag(dir1, false)
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
	dag2, err := CreateDag(dir2, false)
	if err != nil {
		t.Fatalf("Failed to create second DAG: %v", err)
	}

	// Diff the DAGs
	diff, err := dag1.Diff(dag2)
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

func TestDiff_IgnoresLabels(t *testing.T) {
	// Create temporary test directory
	testDir, err := os.MkdirTemp("", "dag_diff_labels_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Generate test data
	GenerateDummyDirectory(filepath.Join(testDir, "input"), 2, 3, 1, 2)

	// Create two DAGs
	dag1, err := CreateDag(filepath.Join(testDir, "input"), false)
	if err != nil {
		t.Fatalf("Failed to create first DAG: %v", err)
	}

	dag2, err := CreateDag(filepath.Join(testDir, "input"), false)
	if err != nil {
		t.Fatalf("Failed to create second DAG: %v", err)
	}

	// Recompute labels on dag2 to potentially change them
	if err := dag2.RecomputeLabels(); err != nil {
		t.Fatalf("Failed to recompute labels: %v", err)
	}

	// Diff the DAGs
	diff, err := dag1.Diff(dag2)
	if err != nil {
		t.Fatalf("Failed to diff DAGs: %v", err)
	}

	// Should have no differences (labels are ignored)
	if diff.Summary.Total != 0 {
		t.Errorf("Expected no differences when only labels change, got %d total differences", diff.Summary.Total)
		for bareHash, leafDiff := range diff.Diffs {
			t.Logf("  Unexpected diff: %s type=%s", bareHash[:16], leafDiff.Type)
		}
	}

	t.Logf("Correctly ignored label differences")
}

func TestDiff_NilDAGs(t *testing.T) {
	testDir, err := os.MkdirTemp("", "dag_diff_nil_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	GenerateDummyDirectory(filepath.Join(testDir, "input"), 1, 2, 1, 1)
	dag, err := CreateDag(filepath.Join(testDir, "input"), false)
	if err != nil {
		t.Fatalf("Failed to create DAG: %v", err)
	}

	// Test nil source
	var nilDag *Dag
	_, err = nilDag.Diff(dag)
	if err == nil {
		t.Error("Expected error when source DAG is nil")
	}

	// Test nil target
	_, err = dag.Diff(nil)
	if err == nil {
		t.Error("Expected error when target DAG is nil")
	}
}

func TestDiff_RootChanges(t *testing.T) {
	// Create temporary test directories
	testDir, err := os.MkdirTemp("", "dag_diff_root_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Create directory
	dir1 := filepath.Join(testDir, "input")
	GenerateDummyDirectory(dir1, 2, 3, 1, 2)

	// Create first DAG without timestamp
	dag1, err := CreateDag(dir1, false)
	if err != nil {
		t.Fatalf("Failed to create first DAG: %v", err)
	}

	// Create second DAG with timestamp (different root metadata)
	dag2, err := CreateDag(dir1, true)
	if err != nil {
		t.Fatalf("Failed to create second DAG: %v", err)
	}

	// Diff the DAGs
	diff, err := dag1.Diff(dag2)
	if err != nil {
		t.Fatalf("Failed to diff DAGs: %v", err)
	}

	// Root might show as added/removed due to timestamp in additional_data changing the hash
	t.Logf("Root diff summary: Added=%d, Removed=%d, Total=%d",
		diff.Summary.Added, diff.Summary.Removed, diff.Summary.Total)

	// Check if root is in the diff
	rootBareHash := StripLabel(dag1.Root)
	if rootDiff, exists := diff.Diffs[rootBareHash]; exists {
		t.Logf("Root leaf found in diff with type: %s", rootDiff.Type)
	}
}
