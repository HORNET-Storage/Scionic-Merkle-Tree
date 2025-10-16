package dag

import (
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestCreateDagCustom(t *testing.T) {
	// Create a temporary test directory
	testDir, err := os.MkdirTemp("", "dag_custom_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(testDir) // Clean up after test

	// Generate test data with a mix of files and directories
	GenerateDummyDirectory(testDir, 3, 5, 2, 3)

	// Define root metadata
	rootMetadata := map[string]string{
		"root_key":  "root_value",
		"timestamp": time.Now().Format(time.RFC3339),
	}

	// Define a processor function that adds metadata based on file/directory properties
	processor := func(path string, relPath string, entry fs.DirEntry, isRoot bool, leafType LeafType) map[string]string {
		// Skip root (it gets rootMetadata directly)
		if isRoot {
			return nil
		}

		metadata := map[string]string{
			"path_length": strconv.Itoa(len(relPath)),
			"is_dir":      strconv.FormatBool(entry.IsDir()),
			"leaf_type":   string(leafType),
		}

		// Add file-specific metadata
		if !entry.IsDir() {
			fileInfo, err := entry.Info()
			if err == nil {
				metadata["file_size"] = strconv.FormatInt(fileInfo.Size(), 10)
				metadata["file_mode"] = fileInfo.Mode().String()
			}
		}

		return metadata
	}

	// Create DAGs with different processors
	customDag, err := CreateDagCustom(testDir, rootMetadata, processor)
	if err != nil {
		t.Fatalf("Failed to create custom DAG: %v", err)
	}

	// Create a DAG with nil processor for comparison
	nilProcessorDag, err := CreateDagCustom(testDir, rootMetadata, nil)
	if err != nil {
		t.Fatalf("Failed to create DAG with nil processor: %v", err)
	}

	// Run subtests
	t.Run("VerifyRootMetadata", func(t *testing.T) {
		// Test that root metadata was correctly added
		rootLeaf := customDag.Leafs[customDag.Root]
		if rootLeaf == nil {
			t.Fatal("Root leaf not found")
		}

		// Check root metadata
		for key, expectedValue := range rootMetadata {
			if value, exists := rootLeaf.AdditionalData[key]; !exists || value != expectedValue {
				t.Errorf("Root metadata mismatch for key %s: expected %s, got %s",
					key, expectedValue, value)
			}
		}
	})

	t.Run("VerifyLeafMetadata", func(t *testing.T) {
		// Test that leaf metadata was correctly added to non-root leaves
		for hash, leaf := range customDag.Leafs {
			if hash == customDag.Root {
				continue // Skip root leaf
			}

			// Every non-root leaf should have metadata
			if leaf.AdditionalData == nil || len(leaf.AdditionalData) == 0 {
				t.Errorf("Leaf %s has no metadata", hash)
				continue
			}

			// Check for expected metadata keys
			expectedKeys := []string{"path_length", "is_dir", "leaf_type"}
			for _, key := range expectedKeys {
				if _, exists := leaf.AdditionalData[key]; !exists {
					t.Errorf("Leaf %s missing expected metadata key: %s", hash, key)
				}
			}

			// File leaves should have file-specific metadata
			if leaf.Type == FileLeafType {
				fileKeys := []string{"file_size", "file_mode"}
				for _, key := range fileKeys {
					if _, exists := leaf.AdditionalData[key]; !exists {
						t.Errorf("File leaf %s missing expected file metadata key: %s", hash, key)
					}
				}
			}
		}
	})

	t.Run("VerifyDagIntegrity", func(t *testing.T) {
		// Test that the DAG can be verified
		if err := customDag.Verify(); err != nil {
			t.Errorf("Custom DAG failed verification: %v", err)
		}
	})

	t.Run("VerifySerialization", func(t *testing.T) {
		// Test serialization and deserialization
		data, err := customDag.ToCBOR()
		if err != nil {
			t.Fatalf("Failed to serialize custom DAG: %v", err)
		}

		deserializedDag, err := FromCBOR(data)
		if err != nil {
			t.Fatalf("Failed to deserialize custom DAG: %v", err)
		}

		// Verify the deserialized DAG
		if err := deserializedDag.Verify(); err != nil {
			t.Errorf("Deserialized DAG failed verification: %v", err)
		}

		// Check that metadata was preserved
		rootLeaf := deserializedDag.Leafs[deserializedDag.Root]
		for key, expectedValue := range rootMetadata {
			if value, exists := rootLeaf.AdditionalData[key]; !exists || value != expectedValue {
				t.Errorf("Deserialized root metadata mismatch for key %s: expected %s, got %s",
					key, expectedValue, value)
			}
		}

		// Check a few non-root leaves to ensure their metadata was preserved
		leafCount := 0
		for hash, leaf := range deserializedDag.Leafs {
			if hash == deserializedDag.Root {
				continue // Skip root leaf
			}

			if leaf.AdditionalData == nil || len(leaf.AdditionalData) == 0 {
				t.Errorf("Deserialized leaf %s has no metadata", hash)
				continue
			}

			// Check that leaf_type matches the actual leaf type
			if leafType, exists := leaf.AdditionalData["leaf_type"]; exists {
				if leafType != string(leaf.Type) {
					t.Errorf("Leaf type mismatch for %s: metadata=%s, actual=%s",
						hash, leafType, leaf.Type)
				}
			}

			leafCount++
			if leafCount >= 3 {
				break // Only check a few leaves to keep the test fast
			}
		}
	})

	t.Run("VerifyRecreation", func(t *testing.T) {
		// Test that the DAG can recreate the directory structure
		outputDir := filepath.Join(testDir, "output")
		if err := customDag.CreateDirectory(outputDir); err != nil {
			t.Errorf("Failed to recreate directory from custom DAG: %v", err)
		}

		// Verify that the output directory exists
		if _, err := os.Stat(outputDir); os.IsNotExist(err) {
			t.Errorf("Output directory was not created")
		}
	})

	t.Run("CompareWithStandardDag", func(t *testing.T) {
		// Create a standard DAG for comparison (without custom processor)
		standardDag, err := CreateDag(testDir, false)
		if err != nil {
			t.Fatalf("Failed to create standard DAG: %v", err)
		}

		// Debug output for leaf counts
		t.Logf("Custom DAG leaf count: %d", len(customDag.Leafs))
		t.Logf("Standard DAG leaf count: %d", len(standardDag.Leafs))
		t.Logf("Nil Processor DAG leaf count: %d", len(nilProcessorDag.Leafs))

		// Count leaf types in each DAG
		customFileCount, customDirCount, customChunkCount := 0, 0, 0
		standardFileCount, standardDirCount, standardChunkCount := 0, 0, 0
		nilFileCount, nilDirCount, nilChunkCount := 0, 0, 0

		for _, leaf := range customDag.Leafs {
			switch leaf.Type {
			case FileLeafType:
				customFileCount++
			case DirectoryLeafType:
				customDirCount++
			case ChunkLeafType:
				customChunkCount++
			}
		}

		for _, leaf := range standardDag.Leafs {
			switch leaf.Type {
			case FileLeafType:
				standardFileCount++
			case DirectoryLeafType:
				standardDirCount++
			case ChunkLeafType:
				standardChunkCount++
			}
		}

		for _, leaf := range nilProcessorDag.Leafs {
			switch leaf.Type {
			case FileLeafType:
				nilFileCount++
			case DirectoryLeafType:
				nilDirCount++
			case ChunkLeafType:
				nilChunkCount++
			}
		}

		t.Logf("Custom DAG leaf types: Files=%d, Dirs=%d, Chunks=%d",
			customFileCount, customDirCount, customChunkCount)
		t.Logf("Standard DAG leaf types: Files=%d, Dirs=%d, Chunks=%d",
			standardFileCount, standardDirCount, standardChunkCount)
		t.Logf("Nil Processor DAG leaf types: Files=%d, Dirs=%d, Chunks=%d",
			nilFileCount, nilDirCount, nilChunkCount)

		// Verify custom DAG and nil processor DAG have the same structure
		// (both use CreateDagCustom, just with different processors)
		if len(customDag.Leafs) != len(nilProcessorDag.Leafs) {
			t.Errorf("Leaf count mismatch between custom and nil processor DAGs: custom=%d, nil=%d",
				len(customDag.Leafs), len(nilProcessorDag.Leafs))
		}

		// Note: Standard DAG (CreateDag) and Custom DAG (CreateDagCustom) may have different
		// leaf counts due to implementation differences. We only verify that the root hashes
		// differ when metadata is added, which is the important behavior.

		// Root hash should be different between custom and standard DAGs due to added metadata
		if customDag.Root == standardDag.Root {
			t.Errorf("Root hashes should differ due to added metadata")
		}

		// Verify all DAGs are valid
		if err := customDag.Verify(); err != nil {
			t.Errorf("Custom DAG verification failed: %v", err)
		}
		if err := standardDag.Verify(); err != nil {
			t.Errorf("Standard DAG verification failed: %v", err)
		}
		if err := nilProcessorDag.Verify(); err != nil {
			t.Errorf("Nil processor DAG verification failed: %v", err)
		}
	})

	t.Run("TestNilProcessor", func(t *testing.T) {
		// Test that CreateDagCustom works with a nil processor
		nilProcessorDag, err := CreateDagCustom(testDir, rootMetadata, nil)
		if err != nil {
			t.Fatalf("Failed to create DAG with nil processor: %v", err)
		}

		// Verify the DAG
		if err := nilProcessorDag.Verify(); err != nil {
			t.Errorf("DAG with nil processor failed verification: %v", err)
		}

		// Only root should have metadata
		rootLeaf := nilProcessorDag.Leafs[nilProcessorDag.Root]
		for key, expectedValue := range rootMetadata {
			if value, exists := rootLeaf.AdditionalData[key]; !exists || value != expectedValue {
				t.Errorf("Root metadata mismatch for key %s: expected %s, got %s",
					key, expectedValue, value)
			}
		}

		// Non-root leaves should not have metadata
		for hash, leaf := range nilProcessorDag.Leafs {
			if hash == nilProcessorDag.Root {
				continue
			}

			// Either AdditionalData should be nil or empty
			if leaf.AdditionalData != nil && len(leaf.AdditionalData) > 0 {
				t.Errorf("Non-root leaf %s has metadata with nil processor", hash)
			}
		}
	})

	t.Run("TestEmptyProcessor", func(t *testing.T) {
		// Test with a processor that returns nil or empty metadata
		emptyProcessor := func(path string, relPath string, entry fs.DirEntry, isRoot bool, leafType LeafType) map[string]string {
			return nil
		}

		// Create a new nil processor DAG specifically for this test
		// to ensure we're comparing DAGs created from the same directory state
		localNilProcessorDag, err := CreateDagCustom(testDir, rootMetadata, nil)
		if err != nil {
			t.Fatalf("Failed to create local nil processor DAG: %v", err)
		}

		emptyProcessorDag, err := CreateDagCustom(testDir, rootMetadata, emptyProcessor)
		if err != nil {
			t.Fatalf("Failed to create DAG with empty processor: %v", err)
		}

		// Verify the DAGs
		if err := emptyProcessorDag.Verify(); err != nil {
			t.Errorf("DAG with empty processor failed verification: %v", err)
		}

		if err := localNilProcessorDag.Verify(); err != nil {
			t.Errorf("Local nil processor DAG failed verification: %v", err)
		}

		// Debug output
		t.Logf("Empty processor DAG leaf count: %d", len(emptyProcessorDag.Leafs))
		t.Logf("Local nil processor DAG leaf count: %d", len(localNilProcessorDag.Leafs))

		// Should be equivalent to using a nil processor
		if emptyProcessorDag.Root != localNilProcessorDag.Root {
			t.Errorf("Root hash mismatch between nil and empty processor DAGs")
			t.Logf("Empty processor DAG root: %s", emptyProcessorDag.Root)
			t.Logf("Local nil processor DAG root: %s", localNilProcessorDag.Root)
		}
	})
}
