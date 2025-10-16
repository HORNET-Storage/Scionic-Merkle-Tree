package dag

import (
	"os"
	"path/filepath"
	"testing"
)

// TestSimpleParallelDeterminism tests with a very simple directory structure
func TestSimpleParallelDeterminism(t *testing.T) {
	// Create temporary test directory structure
	tmpDir, err := os.MkdirTemp("", "simple-parallel-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create simple structure:
	// tmpDir/
	//   ├── a.txt
	//   └── b.txt

	if err := os.WriteFile(filepath.Join(tmpDir, "a.txt"), []byte("a"), 0644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "b.txt"), []byte("b"), 0644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	// Build DAG sequentially
	sequentialConfig := DefaultConfig()
	sequentialDAG, err := CreateDagWithConfig(tmpDir, sequentialConfig)
	if err != nil {
		t.Fatalf("Sequential DAG creation failed: %v", err)
	}

	t.Logf("Sequential DAG root: %s", sequentialDAG.Root)
	t.Logf("Sequential DAG leaves:")
	for label, leaf := range sequentialDAG.Leafs {
		t.Logf("  %s: %s (type=%s, name=%s, links=%d)", label, leaf.Hash, leaf.Type, leaf.ItemName, len(leaf.Links))
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
	t.Logf("Parallel DAG leaves:")
	for label, leaf := range parallelDAG.Leafs {
		t.Logf("  %s: %s (type=%s, name=%s, links=%d)", label, leaf.Hash, leaf.Type, leaf.ItemName, len(leaf.Links))
		for linkLabel, linkHash := range leaf.Links {
			t.Logf("    -> %s: %s", linkLabel, linkHash)
		}
	}

	// Compare
	if sequentialDAG.Root != parallelDAG.Root {
		t.Errorf("Root hashes don't match!\nSequential: %s\nParallel:   %s",
			sequentialDAG.Root, parallelDAG.Root)
	}
}
