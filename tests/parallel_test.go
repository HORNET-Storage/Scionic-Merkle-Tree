package tests

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/HORNET-Storage/Scionic-Merkle-Tree/dag"
)

// TestParallelDeterminism verifies that parallel and sequential DAG building
// produce identical DAG structures (same hashes, labels, and structure)
func TestParallelDeterminism(t *testing.T) {
	// Create temporary test directory structure
	tmpDir, err := os.MkdirTemp("", "parallel-dag-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create test structure:
	// tmpDir/
	//   ├── file1.txt
	//   ├── file2.txt
	//   ├── subdir1/
	//   │   ├── file3.txt
	//   │   └── file4.txt
	//   └── subdir2/
	//       └── file5.txt

	testData := map[string]string{
		"file1.txt":         "Content of file 1",
		"file2.txt":         "Content of file 2",
		"subdir1/file3.txt": "Content of file 3",
		"subdir1/file4.txt": "Content of file 4",
		"subdir2/file5.txt": "Content of file 5",
	}

	for path, content := range testData {
		fullPath := filepath.Join(tmpDir, path)
		dir := filepath.Dir(fullPath)

		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatalf("Failed to create directory %s: %v", dir, err)
		}

		if err := os.WriteFile(fullPath, []byte(content), 0644); err != nil {
			t.Fatalf("Failed to write file %s: %v", fullPath, err)
		}
	}

	// Build DAG sequentially
	sequentialConfig := dag.DefaultConfig()
	sequentialDAG, err := dag.CreateDagWithConfig(tmpDir, sequentialConfig)
	if err != nil {
		t.Fatalf("Sequential DAG creation failed: %v", err)
	}

	// Build DAG in parallel (with 2 workers)
	parallelConfig := dag.ParallelConfigWithWorkers(2)
	parallelDAG, err := dag.CreateDagWithConfig(tmpDir, parallelConfig)
	if err != nil {
		t.Fatalf("Parallel DAG creation failed: %v", err)
	}

	// Compare root hashes (most critical test)
	if sequentialDAG.Root != parallelDAG.Root {
		t.Errorf("Root hashes don't match!\nSequential: %s\nParallel:   %s",
			sequentialDAG.Root, parallelDAG.Root)
	}

	// Compare number of leaves
	if len(sequentialDAG.Leafs) != len(parallelDAG.Leafs) {
		t.Errorf("Different number of leaves: sequential=%d, parallel=%d",
			len(sequentialDAG.Leafs), len(parallelDAG.Leafs))
	}

	// Compare each leaf by label
	for label, seqLeaf := range sequentialDAG.Leafs {
		parLeaf, exists := parallelDAG.Leafs[label]
		if !exists {
			t.Errorf("Label %s exists in sequential but not in parallel", label)
			continue
		}

		if seqLeaf.Hash != parLeaf.Hash {
			t.Errorf("Hash mismatch for label %s:\nSequential: %s\nParallel:   %s",
				label, seqLeaf.Hash, parLeaf.Hash)
		}

		if seqLeaf.Type != parLeaf.Type {
			t.Errorf("Type mismatch for label %s: sequential=%s, parallel=%s",
				label, seqLeaf.Type, parLeaf.Type)
		}

		if seqLeaf.ItemName != parLeaf.ItemName {
			t.Errorf("ItemName mismatch for label %s: sequential=%s, parallel=%s",
				label, seqLeaf.ItemName, parLeaf.ItemName)
		}

		if len(seqLeaf.Links) != len(parLeaf.Links) {
			t.Errorf("Links count mismatch for label %s: sequential=%d, parallel=%d",
				label, len(seqLeaf.Links), len(parLeaf.Links))
		}

		// Compare links (Links is now an array)
		for i, seqHash := range seqLeaf.Links {
			if i >= len(parLeaf.Links) {
				t.Errorf("Link at index %d exists in sequential leaf %s but not in parallel",
					i, label)
				continue
			}
			parHash := parLeaf.Links[i]

			if seqHash != parHash {
				t.Errorf("Link hash mismatch for label %s, link %d:\nSequential: %s\nParallel:   %s",
					label, i, seqHash, parHash)
			}
		}
	}
}

// TestParallelConsistency verifies that running parallel DAG building multiple times
// produces the same result each time (tests for any race conditions)
func TestParallelConsistency(t *testing.T) {
	// Create temporary test directory
	tmpDir, err := os.MkdirTemp("", "parallel-consistency-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create test files
	for i := 0; i < 20; i++ {
		filename := filepath.Join(tmpDir, "file"+string(rune('a'+i))+".txt")
		content := "Test content " + string(rune('a'+i))
		if err := os.WriteFile(filename, []byte(content), 0644); err != nil {
			t.Fatalf("Failed to write file: %v", err)
		}
	}

	config := dag.ParallelConfigWithWorkers(4)

	// Build DAG multiple times
	const iterations = 5
	var rootHashes []string

	for i := 0; i < iterations; i++ {
		dag, err := dag.CreateDagWithConfig(tmpDir, config)
		if err != nil {
			t.Fatalf("Parallel DAG creation failed (iteration %d): %v", i, err)
		}
		rootHashes = append(rootHashes, dag.Root)
	}

	// All root hashes should be identical
	for i := 1; i < iterations; i++ {
		if rootHashes[0] != rootHashes[i] {
			t.Errorf("Inconsistent root hashes between iterations:\nIteration 0: %s\nIteration %d: %s",
				rootHashes[0], i, rootHashes[i])
		}
	}
}

// TestParallelWithDifferentWorkerCounts verifies that different worker counts
// still produce the same DAG
func TestParallelWithDifferentWorkerCounts(t *testing.T) {
	// Create temporary test directory
	tmpDir, err := os.MkdirTemp("", "parallel-workers-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create test structure
	for i := 0; i < 10; i++ {
		filename := filepath.Join(tmpDir, "file"+string(rune('a'+i))+".txt")
		content := "Test content " + string(rune('a'+i))
		if err := os.WriteFile(filename, []byte(content), 0644); err != nil {
			t.Fatalf("Failed to write file: %v", err)
		}
	}

	// Test with different worker counts
	workerCounts := []int{1, 2, 4, 8}
	var rootHashes []string

	for _, workers := range workerCounts {
		config := dag.ParallelConfigWithWorkers(workers)
		dag, err := dag.CreateDagWithConfig(tmpDir, config)
		if err != nil {
			t.Fatalf("Parallel DAG creation failed (workers=%d): %v", workers, err)
		}
		rootHashes = append(rootHashes, dag.Root)
	}

	// All root hashes should be identical regardless of worker count
	for i := 1; i < len(workerCounts); i++ {
		if rootHashes[0] != rootHashes[i] {
			t.Errorf("Inconsistent root hashes with different worker counts:\nWorkers %d: %s\nWorkers %d: %s",
				workerCounts[0], rootHashes[0], workerCounts[i], rootHashes[i])
		}
	}
}
