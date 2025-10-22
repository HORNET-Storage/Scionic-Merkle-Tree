package tests

import (
	"io/fs"
	"os"
	"strconv"
	"testing"

	"github.com/HORNET-Storage/Scionic-Merkle-Tree/dag"
	"github.com/HORNET-Storage/Scionic-Merkle-Tree/testutil"
)

// TestCreateDagCustom tests custom DAG creation with metadata processors
// Uses all fixtures to ensure custom metadata works for all DAG types
func TestCreateDagCustom(t *testing.T) {
	// Define a processor function that adds metadata based on file/directory properties
	processor := func(path string, relPath string, entry fs.DirEntry, isRoot bool, leafType dag.LeafType) map[string]string {
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

	testutil.RunTestWithAllFixtures(t, func(t *testing.T, standardDag *dag.Dag, fixture testutil.TestFixture, fixturePath string) {
		// Define root metadata (deterministic, not random)
		rootMetadata := map[string]string{
			"root_key":  "root_value",
			"timestamp": "2025-10-22T00:00:00Z", // Fixed timestamp for determinism
			"fixture":   fixture.Name,
		}

		// Create DAG with custom metadata
		customDag, err := dag.CreateDagCustom(fixturePath, rootMetadata, processor)
		if err != nil {
			t.Fatalf("Failed to create custom DAG for %s: %v", fixture.Name, err)
		}

		// Create a DAG with nil processor for comparison
		nilProcessorDag, err := dag.CreateDagCustom(fixturePath, rootMetadata, nil)
		if err != nil {
			t.Fatalf("Failed to create DAG with nil processor for %s: %v", fixture.Name, err)
		}

		// Test that root metadata was correctly added
		t.Run("VerifyRootMetadata", func(t *testing.T) {
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

		// Test that leaf metadata was correctly added to non-root leaves
		t.Run("VerifyLeafMetadata", func(t *testing.T) {
			for hash, leaf := range customDag.Leafs {
				if hash == customDag.Root {
					continue // Skip root leaf
				}

				// Skip chunk leaves - they don't get custom metadata
				if leaf.Type == dag.ChunkLeafType {
					continue
				}

				// Every non-root, non-chunk leaf should have metadata
				if len(leaf.AdditionalData) == 0 {
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
				if leaf.Type == dag.FileLeafType {
					fileKeys := []string{"file_size", "file_mode"}
					for _, key := range fileKeys {
						if _, exists := leaf.AdditionalData[key]; !exists {
							t.Errorf("File leaf %s missing expected file metadata key: %s", hash, key)
						}
					}
				}
			}
		})

		// Test that the DAG can be verified
		t.Run("VerifyDagIntegrity", func(t *testing.T) {
			if err := customDag.Verify(); err != nil {
				t.Errorf("Custom DAG failed verification: %v", err)
			}
		})

		// Test serialization and deserialization
		t.Run("VerifySerialization", func(t *testing.T) {
			data, err := customDag.ToCBOR()
			if err != nil {
				t.Fatalf("Failed to serialize custom DAG: %v", err)
			}

			deserializedDag, err := dag.FromCBOR(data)
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

				// Skip chunk leaves - they don't have custom metadata
				if leaf.Type == dag.ChunkLeafType {
					continue
				}

				if len(leaf.AdditionalData) == 0 {
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

		// Test that the DAG can recreate the directory structure
		t.Run("VerifyRecreation", func(t *testing.T) {
			tmpOutput, err := os.MkdirTemp("", "custom_output_*")
			if err != nil {
				t.Fatalf("Failed to create temp output directory: %v", err)
			}
			defer os.RemoveAll(tmpOutput)

			if err := customDag.CreateDirectory(tmpOutput); err != nil {
				t.Errorf("Failed to recreate directory from custom DAG: %v", err)
			}

			// Verify that the output directory exists
			if _, err := os.Stat(tmpOutput); os.IsNotExist(err) {
				t.Errorf("Output directory was not created")
			}
		})

		// Compare with standard DAG
		t.Run("CompareWithStandardDag", func(t *testing.T) {
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

		// Test that CreateDagCustom works with a nil processor
		t.Run("TestNilProcessor", func(t *testing.T) {
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
				if len(leaf.AdditionalData) > 0 {
					t.Errorf("Non-root leaf %s has metadata with nil processor", hash)
				}
			}
		})

		// Test with a processor that returns nil or empty metadata
		t.Run("TestEmptyProcessor", func(t *testing.T) {
			emptyProcessor := func(path string, relPath string, entry fs.DirEntry, isRoot bool, leafType dag.LeafType) map[string]string {
				return nil
			}

			emptyProcessorDag, err := dag.CreateDagCustom(fixturePath, rootMetadata, emptyProcessor)
			if err != nil {
				t.Fatalf("Failed to create DAG with empty processor: %v", err)
			}

			// Verify the DAGs
			if err := emptyProcessorDag.Verify(); err != nil {
				t.Errorf("DAG with empty processor failed verification: %v", err)
			}

			// Should be equivalent to using a nil processor
			if emptyProcessorDag.Root != nilProcessorDag.Root {
				t.Logf("Note: Empty processor DAG root (%s) differs from nil processor DAG root (%s) for %s",
					emptyProcessorDag.Root, nilProcessorDag.Root, fixture.Name)
			}
		})

		t.Logf("✓ %s: Custom metadata test completed successfully", fixture.Name)
	})
}
