package dag

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOutOfRangeLeafRequests(t *testing.T) {
	// Create a simple DAG with known number of leaves
	tmpDir, err := ioutil.TempDir("", "test")
	if err != nil {
		t.Fatalf("Could not create temp directory: %s", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create 5 test files
	for i := 0; i < 5; i++ {
		err := ioutil.WriteFile(
			filepath.Join(tmpDir, string(rune('a'+i))),
			[]byte("test content"),
			0644,
		)
		if err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
	}

	dag, err := CreateDag(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create DAG: %v", err)
	}

	tests := []struct {
		name  string
		start int
		end   int
	}{
		{"beyond_size", 10, 15},
		{"negative_start", -1, 3},
		{"negative_end", 0, -1},
		{"start_greater_than_end", 3, 2},
		{"extremely_large", 1000000, 1000001},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			partial, err := dag.GetPartial(tt.start, tt.end)
			if err != nil {
				return // Expected for invalid ranges
			}
			// If we got a partial DAG, verify it's valid
			if err := partial.VerifyPartial(); err != nil {
				t.Errorf("Invalid partial DAG returned for range %d-%d: %v", tt.start, tt.end, err)
			}
		})
	}
}

func TestSingleFileScenarios(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "test")
	if err != nil {
		t.Fatalf("Could not create temp directory: %s", err)
	}
	defer os.RemoveAll(tmpDir)

	// Test cases for different file sizes and content
	tests := []struct {
		name     string
		size     int
		content  []byte
		filename string
	}{
		{
			name:     "empty_file",
			size:     0,
			content:  []byte{},
			filename: "empty.txt",
		},
		{
			name:     "small_file",
			size:     1024, // 1KB
			filename: "small.txt",
		},
		{
			name:     "exact_chunk_size",
			size:     ChunkSize,
			filename: "exact.txt",
		},
		{
			name:     "larger_than_chunk",
			size:     ChunkSize * 2,
			filename: "large.txt",
		},
		{
			name:     "special_chars",
			size:     1024,
			filename: "special @#$%^&.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filePath := filepath.Join(tmpDir, tt.filename)

			// Generate content if not provided
			content := tt.content
			if len(content) == 0 && tt.size > 0 {
				content = bytes.Repeat([]byte("a"), tt.size)
			}

			// Create the test file
			err := ioutil.WriteFile(filePath, content, 0644)
			if err != nil {
				t.Fatalf("Failed to create test file: %v", err)
			}

			// Create DAG from single file
			dag, err := CreateDag(filePath, false)
			if err != nil {
				t.Fatalf("Failed to create DAG: %v", err)
			}

			// Verify DAG
			if err := dag.Verify(); err != nil {
				t.Errorf("DAG verification failed: %v", err)
			}

			// For files larger than chunk size, verify chunking
			if tt.size > ChunkSize {
				expectedChunks := (tt.size + ChunkSize - 1) / ChunkSize
				var chunkCount int
				for _, leaf := range dag.Leafs {
					if leaf.Type == ChunkLeafType {
						chunkCount++
					}
				}
				if chunkCount != expectedChunks {
					t.Errorf("Expected %d chunks, got %d", expectedChunks, chunkCount)
				}
			}

			// For single file DAGs, verify content
			rootLeaf := dag.Leafs[dag.Root]
			if rootLeaf == nil {
				t.Fatal("Could not find root leaf")
			}

			// Get and verify the content
			recreated, err := dag.GetContentFromLeaf(rootLeaf)
			if err != nil {
				t.Fatalf("Failed to get content from leaf: %v", err)
			}

			// For debugging
			t.Logf("Root leaf type: %s", rootLeaf.Type)
			t.Logf("Root leaf links: %d", len(rootLeaf.Links))
			t.Logf("Content sizes - Original: %d, Recreated: %d", len(content), len(recreated))

			if !bytes.Equal(recreated, content) {
				// Print first few bytes of both for comparison
				maxLen := 50
				origLen := len(content)
				recLen := len(recreated)
				if origLen < maxLen {
					maxLen = origLen
				}
				if recLen < maxLen {
					maxLen = recLen
				}

				t.Errorf("Recreated content does not match original.\nOriginal first %d bytes: %v\nRecreated first %d bytes: %v",
					maxLen, content[:maxLen],
					maxLen, recreated[:maxLen])
			}
		})
	}
}

func TestInvalidPaths(t *testing.T) {
	tests := []struct {
		name string
		path string
	}{
		{
			name: "nonexistent_path",
			path: "/path/that/does/not/exist",
		},
		{
			name: "invalid_chars_windows",
			path: strings.ReplaceAll(filepath.Join(os.TempDir(), "test<>:\"/\\|?*"), "/", string(filepath.Separator)),
		},
		{
			name: "too_long_path",
			path: strings.Repeat("a", 32768), // Exceeds most systems' PATH_MAX
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := CreateDag(tt.path, false)
			if err == nil {
				t.Error("Expected error for invalid path, got nil")
			}
		})
	}
}

func TestBrokenDags(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "test")
	if err != nil {
		t.Fatalf("Could not create temp directory: %s", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a valid DAG first
	err = ioutil.WriteFile(filepath.Join(tmpDir, "test.txt"), []byte("test content"), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	dag, err := CreateDag(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create DAG: %v", err)
	}

	t.Run("missing_leaf", func(t *testing.T) {
		brokenDag := &Dag{
			Root:  dag.Root,
			Leafs: make(map[string]*DagLeaf),
		}
		// Copy all leaves except one
		var skippedOne bool
		for hash, leaf := range dag.Leafs {
			if !skippedOne {
				skippedOne = true
				continue
			}
			brokenDag.Leafs[hash] = leaf
		}
		if err := brokenDag.Verify(); err == nil {
			t.Error("Expected verification to fail for DAG with missing leaf")
		}
	})

	t.Run("corrupted_content", func(t *testing.T) {
		brokenDag := &Dag{
			Root:  dag.Root,
			Leafs: make(map[string]*DagLeaf),
		}
		// Copy all leaves but corrupt one's content
		for hash, leaf := range dag.Leafs {
			leafCopy := leaf.Clone()
			if leaf.Type == FileLeafType || leaf.Type == ChunkLeafType {
				leafCopy.Content = append(leafCopy.Content, []byte("corrupted")...)
			}
			brokenDag.Leafs[hash] = leafCopy
		}
		if err := brokenDag.Verify(); err == nil {
			t.Error("Expected verification to fail for DAG with corrupted content")
		}
	})

	t.Run("invalid_merkle_proof", func(t *testing.T) {
		brokenDag := &Dag{
			Root:  dag.Root,
			Leafs: make(map[string]*DagLeaf),
		}
		// Copy all leaves but corrupt merkle root
		for hash, leaf := range dag.Leafs {
			leafCopy := leaf.Clone()
			if len(leafCopy.ClassicMerkleRoot) > 0 {
				leafCopy.ClassicMerkleRoot = append(leafCopy.ClassicMerkleRoot, []byte("corrupted")...)
			}
			brokenDag.Leafs[hash] = leafCopy
		}
		if err := brokenDag.Verify(); err == nil {
			t.Error("Expected verification to fail for DAG with invalid merkle proof")
		}
	})

	t.Run("broken_parent_child", func(t *testing.T) {
		brokenDag := &Dag{
			Root:  dag.Root,
			Leafs: make(map[string]*DagLeaf),
		}
		// Copy all leaves but modify parent-child relationship
		for hash, leaf := range dag.Leafs {
			leafCopy := leaf.Clone()
			if len(leafCopy.Links) > 0 {
				// Add invalid link
				leafCopy.Links["invalid"] = "invalid:hash"
			}
			brokenDag.Leafs[hash] = leafCopy
		}
		if err := brokenDag.Verify(); err == nil {
			t.Error("Expected verification to fail for DAG with broken parent-child relationship")
		}
	})
}
