package tests

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/HORNET-Storage/Scionic-Merkle-Tree/v2/dag"
	"github.com/HORNET-Storage/Scionic-Merkle-Tree/v2/testutil"
)

// TestSerialization tests serialization and deserialization of DAGs in various formats
// Tests against all fixtures to ensure serialization works for all DAG types
func TestSerialization(t *testing.T) {
	testutil.RunTestWithAllFixtures(t, func(t *testing.T, originalDag *dag.Dag, fixture testutil.TestFixture, fixturePath string) {
		tmpDir, err := os.MkdirTemp("", "serialize_output_*")
		if err != nil {
			t.Fatalf("Failed to create temp directory: %v", err)
		}
		defer os.RemoveAll(tmpDir)

		t.Run("CBOR", func(t *testing.T) {
			// Serialize to CBOR
			data, err := originalDag.ToCBOR()
			if err != nil {
				t.Fatalf("Failed to serialize DAG to CBOR: %v", err)
			}

			// Deserialize from CBOR
			deserializedDag, err := dag.FromCBOR(data)
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
			outputDir := filepath.Join(tmpDir, "cbor_output")
			if err := deserializedDag.CreateDirectory(outputDir); err != nil {
				t.Errorf("Failed to recreate directory from deserialized DAG: %v", err)
			}
		})

		t.Run("Partial_DAG", func(t *testing.T) {
			// Get a file leaf hash from the DAG
			var fileHash string
			for hash, leaf := range originalDag.Leafs {
				if leaf.Type == dag.FileLeafType {
					fileHash = hash
					break
				}
			}
			if fileHash == "" {
				t.Skip("No file leaves in DAG to test partial")
			}

			// Get a partial DAG with that one file
			partialDag, err := originalDag.GetPartial([]string{fileHash}, true)
			if err != nil {
				t.Fatalf("Failed to get partial DAG: %v", err)
			}

			// Skip if the partial is actually the complete DAG (happens with single file fixtures)
			if !partialDag.IsPartial() {
				t.Skip("Partial DAG is identical to full DAG, skipping partial serialization test")
			}

			// Verify the partial DAG before serialization
			if err := partialDag.Verify(); err != nil {
				t.Fatalf("Partial DAG failed verification before serialization: %v", err)
			}

			// Serialize to JSON
			data, err := partialDag.ToJSON()
			if err != nil {
				t.Fatalf("Failed to serialize partial DAG to JSON: %v", err)
			}

			// Deserialize from JSON
			deserializedDag, err := dag.FromJSON(data)
			if err != nil {
				t.Fatalf("Failed to deserialize partial DAG from JSON: %v", err)
			}

			// Verify the deserialized partial DAG
			if err := deserializedDag.Verify(); err != nil {
				t.Errorf("Deserialized partial DAG failed verification: %v", err)
				t.Log("Original partial DAG:")
				for hash, leaf := range partialDag.Leafs {
					t.Logf("Leaf %s: Type=%s Links=%d Proofs=%d", hash, leaf.Type, len(leaf.Links), len(leaf.Proofs))
				}
				t.Log("\nDeserialized partial DAG:")
				for hash, leaf := range deserializedDag.Leafs {
					t.Logf("Leaf %s: Type=%s Links=%d Proofs=%d", hash, leaf.Type, len(leaf.Links), len(leaf.Proofs))
				}
			}

			// Verify it's still recognized as a partial DAG
			if !deserializedDag.IsPartial() {
				t.Error("Deserialized DAG not recognized as partial")
			}
		})

		t.Run("JSON", func(t *testing.T) {
			// Serialize to JSON
			data, err := originalDag.ToJSON()
			if err != nil {
				t.Fatalf("Failed to serialize DAG to JSON: %v", err)
			}

			// Deserialize from JSON
			deserializedDag, err := dag.FromJSON(data)
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
			outputDir := filepath.Join(tmpDir, "json_output")
			if err := deserializedDag.CreateDirectory(outputDir); err != nil {
				t.Errorf("Failed to recreate directory from deserialized DAG: %v", err)
			}
		})

		t.Run("TransmissionPacket", func(t *testing.T) {
			// Get a sequence of transmission packets
			sequence := originalDag.GetLeafSequence()
			if len(sequence) == 0 {
				t.Fatal("No transmission packets generated")
			}

			// Test the first packet
			packet := sequence[0]

			// Serialize to JSON
			jsonData, err := packet.ToJSON()
			if err != nil {
				t.Fatalf("Failed to serialize TransmissionPacket to JSON: %v", err)
			}

			// Deserialize from JSON
			deserializedPacket, err := dag.TransmissionPacketFromJSON(jsonData)
			if err != nil {
				t.Fatalf("Failed to deserialize TransmissionPacket from JSON: %v", err)
			}

			// Verify the deserialized packet
			if packet.Leaf.Hash != deserializedPacket.Leaf.Hash {
				t.Errorf("Leaf hash mismatch: expected %s, got %s", packet.Leaf.Hash, deserializedPacket.Leaf.Hash)
			}
			if packet.ParentHash != deserializedPacket.ParentHash {
				t.Errorf("Parent hash mismatch: expected %s, got %s", packet.ParentHash, deserializedPacket.ParentHash)
			}
			if len(packet.Proofs) != len(deserializedPacket.Proofs) {
				t.Errorf("Proofs count mismatch: expected %d, got %d", len(packet.Proofs), len(deserializedPacket.Proofs))
			}

			// Serialize to CBOR
			cborData, err := packet.ToCBOR()
			if err != nil {
				t.Fatalf("Failed to serialize TransmissionPacket to CBOR: %v", err)
			}

			// Deserialize from CBOR
			deserializedPacket, err = dag.TransmissionPacketFromCBOR(cborData)
			if err != nil {
				t.Fatalf("Failed to deserialize TransmissionPacket from CBOR: %v", err)
			}

			// Verify the deserialized packet
			if packet.Leaf.Hash != deserializedPacket.Leaf.Hash {
				t.Errorf("Leaf hash mismatch: expected %s, got %s", packet.Leaf.Hash, deserializedPacket.Leaf.Hash)
			}
			if packet.ParentHash != deserializedPacket.ParentHash {
				t.Errorf("Parent hash mismatch: expected %s, got %s", packet.ParentHash, deserializedPacket.ParentHash)
			}
			if len(packet.Proofs) != len(deserializedPacket.Proofs) {
				t.Errorf("Proofs count mismatch: expected %d, got %d", len(packet.Proofs), len(deserializedPacket.Proofs))
			}
		})
	})
}
