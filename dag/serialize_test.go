package dag

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSerialization(t *testing.T) {
	// Create a temporary test directory
	testDir, err := os.MkdirTemp("", "dag_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(testDir) // Clean up after test

	// Generate test data
	GenerateDummyDirectory(testDir, 3, 2) // 3 items max per dir, 2 levels deep

	// Create a test DAG
	originalDag, err := CreateDag(testDir, false)
	if err != nil {
		t.Fatalf("Failed to create DAG: %v", err)
	}

	t.Run("CBOR", func(t *testing.T) {
		// Serialize to CBOR
		data, err := originalDag.ToCBOR()
		if err != nil {
			t.Fatalf("Failed to serialize DAG to CBOR: %v", err)
		}

		// Deserialize from CBOR
		deserializedDag, err := FromCBOR(data)
		if err != nil {
			t.Fatalf("Failed to deserialize DAG from CBOR: %v", err)
		}

		// Verify the deserialized DAG
		if err := deserializedDag.Verify(); err != nil {
			t.Errorf("Deserialized DAG failed verification: %v", err)
			t.Log("Original DAG:")
			for _, leaf := range originalDag.Leafs {
				t.Logf("Leaf %s: Type=%s Links=%d", leaf.Hash, leaf.Type, len(leaf.Links))
			}
			t.Log("\nDeserialized DAG:")
			for _, leaf := range deserializedDag.Leafs {
				t.Logf("Leaf %s: Type=%s Links=%d", leaf.Hash, leaf.Type, len(leaf.Links))
			}
		}

		// Verify we can recreate the directory structure
		outputDir := filepath.Join(testDir, "cbor_output")
		if err := deserializedDag.CreateDirectory(outputDir); err != nil {
			t.Errorf("Failed to recreate directory from deserialized DAG: %v", err)
		}
	})

	t.Run("JSON", func(t *testing.T) {
		// Serialize to JSON
		data, err := originalDag.ToJSON()
		if err != nil {
			t.Fatalf("Failed to serialize DAG to JSON: %v", err)
		}

		// Deserialize from JSON
		deserializedDag, err := FromJSON(data)
		if err != nil {
			t.Fatalf("Failed to deserialize DAG from JSON: %v", err)
		}

		// Verify the deserialized DAG
		if err := deserializedDag.Verify(); err != nil {
			t.Errorf("Deserialized DAG failed verification: %v", err)
			t.Log("Original DAG:")
			for _, leaf := range originalDag.Leafs {
				t.Logf("Leaf %s: Type=%s Links=%d", leaf.Hash, leaf.Type, len(leaf.Links))
			}
			t.Log("\nDeserialized DAG:")
			for _, leaf := range deserializedDag.Leafs {
				t.Logf("Leaf %s: Type=%s Links=%d", leaf.Hash, leaf.Type, len(leaf.Links))
			}
		}

		// Verify we can recreate the directory structure
		outputDir := filepath.Join(testDir, "json_output")
		if err := deserializedDag.CreateDirectory(outputDir); err != nil {
			t.Errorf("Failed to recreate directory from deserialized DAG: %v", err)
		}
	})
}
