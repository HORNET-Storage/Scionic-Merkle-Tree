package dag

import (
	"os"
	"path/filepath"
	"testing"
)

// TestNestedParallelDeterminism tests with nested directories
func TestNestedParallelDeterminism(t *testing.T) {
	// Create temporary test directory structure
	tmpDir, err := os.MkdirTemp("", "nested-parallel-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create structure:
	// tmpDir/
	//   ├── a.txt
	//   └── subdir/
	//       └── b.txt

	if err := os.WriteFile(filepath.Join(tmpDir, "a.txt"), []byte("a"), 0644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	subdir := filepath.Join(tmpDir, "subdir")
	if err := os.Mkdir(subdir, 0755); err != nil {
		t.Fatalf("Failed to create subdir: %v", err)
	}

	if err := os.WriteFile(filepath.Join(subdir, "b.txt"), []byte("b"), 0644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	// Build DAG sequentially
	sequentialConfig := DefaultConfig()
	sequentialDAG, err := CreateDagWithConfig(tmpDir, sequentialConfig)
	if err != nil {
		t.Fatalf("Sequential DAG creation failed: %v", err)
	}

	t.Logf("Sequential DAG root: %s", sequentialDAG.Root)
	t.Logf("Sequential DAG leaves (%d):", len(sequentialDAG.Leafs))
	for label, leaf := range sequentialDAG.Leafs {
		t.Logf("  %s: type=%s, name=%s, links=%d", label, leaf.Type, leaf.ItemName, len(leaf.Links))
		for linkLabel, linkHash := range leaf.Links {
			t.Logf("    -> %s: %s", linkLabel, linkHash)
		}
	}

	// Build DAG in parallel
	parallelConfig := ParallelConfigWithWorkers(2)
	parallelDAG, err := CreateDagWithConfig(tmpDir, parallelConfig)
	if err != nil {
		t.Fatalf("Parallel DAG creation failed: %v", err)
	}

	t.Logf("\nParallel DAG root: %s", parallelDAG.Root)
	t.Logf("Parallel DAG leaves (%d):", len(parallelDAG.Leafs))
	for label, leaf := range parallelDAG.Leafs {
		t.Logf("  %s: type=%s, name=%s, links=%d", label, leaf.Type, leaf.ItemName, len(leaf.Links))
		for linkLabel, linkHash := range leaf.Links {
			t.Logf("    -> %s: %s", linkLabel, linkHash)
		}
	}

	// Compare
	if sequentialDAG.Root != parallelDAG.Root {
		t.Errorf("Root hashes don't match!\nSequential: %s\nParallel:   %s",
			sequentialDAG.Root, parallelDAG.Root)
	}

	if len(sequentialDAG.Leafs) != len(parallelDAG.Leafs) {
		t.Errorf("Different number of leaves: sequential=%d, parallel=%d",
			len(sequentialDAG.Leafs), len(parallelDAG.Leafs))
	}
}
