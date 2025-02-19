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
			if err := partial.Verify(); err != nil {
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
	// Create a valid DAG with known structure
	dagBuilder := CreateDagBuilder()

	// Create a file leaf
	fileBuilder := CreateDagLeafBuilder("test.txt")
	fileBuilder.SetType(FileLeafType)
	fileBuilder.SetData([]byte("test content"))
	fileLeaf, err := fileBuilder.BuildLeaf(nil)
	if err != nil {
		t.Fatalf("Failed to build file leaf: %v", err)
	}
	fileLeaf.SetLabel("1")
	dagBuilder.AddLeaf(fileLeaf, nil)

	// Create a directory with the file
	dirBuilder := CreateDagLeafBuilder("testdir")
	dirBuilder.SetType(DirectoryLeafType)
	dirBuilder.AddLink("1", fileLeaf.Hash)
	dirLeaf, err := dirBuilder.BuildRootLeaf(dagBuilder, nil)
	if err != nil {
		t.Fatalf("Failed to build directory leaf: %v", err)
	}
	dagBuilder.AddLeaf(dirLeaf, nil)

	dag := dagBuilder.BuildDag(dirLeaf.Hash)

	t.Run("missing_leaf", func(t *testing.T) {
		brokenDag := &Dag{
			Root:  dag.Root,
			Leafs: make(map[string]*DagLeaf),
		}
		// Only copy the root leaf
		brokenDag.Leafs[dag.Root] = dag.Leafs[dag.Root].Clone()

		if err := brokenDag.Verify(); err == nil {
			t.Error("Expected verification to fail for DAG with missing leaf")
		}
	})

	t.Run("corrupted_content", func(t *testing.T) {
		brokenDag := &Dag{
			Root:  dag.Root,
			Leafs: make(map[string]*DagLeaf),
		}
		// Copy all leaves but corrupt file content
		for hash, leaf := range dag.Leafs {
			leafCopy := leaf.Clone()
			if leaf.Type == FileLeafType {
				// Create a new leaf with corrupted content
				builder := CreateDagLeafBuilder(leaf.ItemName)
				builder.SetType(leaf.Type)
				builder.SetData(append(leaf.Content, []byte("corrupted")...))
				corruptedLeaf, _ := builder.BuildLeaf(nil)
				// Keep original hash but use corrupted content and hash
				leafCopy.Content = corruptedLeaf.Content
				leafCopy.ContentHash = corruptedLeaf.ContentHash
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
				// Create a different merkle root by changing the content
				builder := CreateDagLeafBuilder(leaf.ItemName)
				builder.SetType(leaf.Type)
				builder.AddLink("invalid", "invalid:hash")
				corruptedLeaf, _ := builder.BuildLeaf(nil)
				leafCopy.ClassicMerkleRoot = corruptedLeaf.ClassicMerkleRoot
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
				// Add invalid link while preserving CurrentLinkCount
				builder := CreateDagLeafBuilder(leaf.ItemName)
				builder.SetType(leaf.Type)
				builder.AddLink("invalid", "invalid:hash")
				corruptedLeaf, _ := builder.BuildLeaf(nil)
				leafCopy.Links = corruptedLeaf.Links
				// CurrentLinkCount stays the same as it's part of the hash
			}
			brokenDag.Leafs[hash] = leafCopy
		}
		if err := brokenDag.Verify(); err == nil {
			t.Error("Expected verification to fail for DAG with broken parent-child relationship")
		}
	})
}
