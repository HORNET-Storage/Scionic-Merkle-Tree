package tree

import (
	"sort"
	"testing"

	mt "github.com/HORNET-Storage/Scionic-Merkle-Tree/merkletree"
)

func TestBasicTreeOperations(t *testing.T) {
	t.Run("empty tree", func(t *testing.T) {
		tree := CreateTree()
		if len(tree.leafs) != 0 {
			t.Error("New tree should be empty")
		}
	})

	t.Run("single leaf", func(t *testing.T) {
		tree := CreateTree()
		tree.AddLeaf("key1", "data1")

		// Single leaf should error since merkle tree needs at least 2 leaves
		_, _, err := tree.Build()
		if err == nil {
			t.Error("Expected error for single leaf tree")
		}
	})

	t.Run("multiple leaves", func(t *testing.T) {
		tree := CreateTree()
		tree.AddLeaf("key1", "data1")
		tree.AddLeaf("key2", "data2")
		tree.AddLeaf("key3", "data3")

		merkleTree, leafMap, err := tree.Build()
		if err != nil {
			t.Fatalf("Failed to build tree: %v", err)
		}

		if len(merkleTree.Proofs) != 3 {
			t.Errorf("Expected 3 proofs, got %d", len(merkleTree.Proofs))
		}

		if len(leafMap) != 3 {
			t.Errorf("Expected 3 leaves in map, got %d", len(leafMap))
		}
	})
}

func TestProofVerification(t *testing.T) {
	t.Run("verify all proofs", func(t *testing.T) {
		tree := CreateTree()
		tree.AddLeaf("key1", "data1")
		tree.AddLeaf("key2", "data2")
		tree.AddLeaf("key3", "data3")

		merkleTree, leafMap, err := tree.Build()
		if err != nil {
			t.Fatalf("Failed to build tree: %v", err)
		}

		// Convert map to slice for verification
		// Sort leaves by key to match proof order
		var keys []string
		for k := range leafMap {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		var leafs []mt.DataBlock
		for _, key := range keys {
			leafs = append(leafs, leafMap[key])
		}

		if !VerifyTree(merkleTree, leafs) {
			t.Error("Tree verification failed")
		}
	})

	t.Run("verify root", func(t *testing.T) {
		tree := CreateTree()
		tree.AddLeaf("key1", "data1")
		tree.AddLeaf("key2", "data2")
		tree.AddLeaf("key3", "data3")

		merkleTree, leafMap, err := tree.Build()
		if err != nil {
			t.Fatalf("Failed to build tree: %v", err)
		}

		// Sort leaves by key to match proof order
		var keys []string
		for k := range leafMap {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		var leafs []mt.DataBlock
		for _, key := range keys {
			leafs = append(leafs, leafMap[key])
		}

		if !VerifyRoot(merkleTree.Root, merkleTree.Proofs, leafs) {
			t.Error("Root verification failed")
		}
	})

	t.Run("verify modified data fails", func(t *testing.T) {
		tree := CreateTree()
		tree.AddLeaf("key1", "data1")
		tree.AddLeaf("key2", "data2")

		merkleTree, _, err := tree.Build()
		if err != nil {
			t.Fatalf("Failed to build tree: %v", err)
		}

		// Create modified leaf
		modifiedLeaf := CreateLeaf("modified_data")

		// Try to verify with modified data
		err = merkleTree.Verify(modifiedLeaf, merkleTree.Proofs[0])
		if err == nil {
			t.Error("Expected verification to fail with modified data")
		}
	})
}

func TestKeyFeatures(t *testing.T) {
	t.Run("get index for key", func(t *testing.T) {
		tree := CreateTree()
		tree.AddLeaf("key1", "data1")
		tree.AddLeaf("key2", "data2")

		merkleTree, _, err := tree.Build()
		if err != nil {
			t.Fatalf("Failed to build tree: %v", err)
		}

		index, exists := merkleTree.GetIndexForKey("key1")
		if !exists {
			t.Error("Failed to find index for key1")
		}

		// Verify proof using index
		proof := merkleTree.Proofs[index]
		leaf := CreateLeaf("data1")
		err = merkleTree.Verify(leaf, proof)
		if err != nil {
			t.Errorf("Verification failed for key-based proof: %v", err)
		}
	})

	t.Run("nonexistent key", func(t *testing.T) {
		tree := CreateTree()
		tree.AddLeaf("key1", "data1")
		tree.AddLeaf("key2", "data2") // Add second leaf to meet minimum requirement

		merkleTree, _, err := tree.Build()
		if err != nil {
			t.Fatalf("Failed to build tree: %v", err)
		}

		_, exists := merkleTree.GetIndexForKey("nonexistent")
		if exists {
			t.Error("GetIndexForKey should return false for nonexistent key")
		}
	})
}

func TestEdgeCases(t *testing.T) {
	t.Run("empty tree build", func(t *testing.T) {
		tree := CreateTree()
		_, _, err := tree.Build()
		if err == nil {
			t.Error("Expected error when building empty tree")
		}
	})

	t.Run("single leaf tree", func(t *testing.T) {
		tree := CreateTree()
		tree.AddLeaf("key1", "data1")

		// Single leaf should error since merkle tree needs at least 2 leaves
		_, _, err := tree.Build()
		if err == nil {
			t.Error("Expected error for single leaf tree")
		}
	})

	t.Run("duplicate data", func(t *testing.T) {
		tree := CreateTree()
		tree.AddLeaf("key1", "same_data")
		tree.AddLeaf("key2", "same_data")

		merkleTree, leafMap, err := tree.Build()
		if err != nil {
			t.Fatalf("Failed to build tree with duplicate data: %v", err)
		}

		// Both leaves should verify with their respective proofs
		leaf1 := leafMap["key1"]
		leaf2 := leafMap["key2"]

		// Verify both proofs
		err1 := merkleTree.Verify(leaf1, merkleTree.Proofs[0])
		err2 := merkleTree.Verify(leaf2, merkleTree.Proofs[1])

		if err1 != nil || err2 != nil {
			t.Error("Verification failed for duplicate data")
		}

		// Verify that modifying one leaf's data breaks verification
		modifiedLeaf := CreateLeaf("modified_data")
		err = merkleTree.Verify(modifiedLeaf, merkleTree.Proofs[0])
		if err == nil {
			t.Error("Verification should fail with modified data")
		}
	})
}

func TestErrorCases(t *testing.T) {
	t.Run("wrong proof", func(t *testing.T) {
		tree := CreateTree()
		tree.AddLeaf("key1", "data1")
		tree.AddLeaf("key2", "data2")

		merkleTree, leafMap, err := tree.Build()
		if err != nil {
			t.Fatalf("Failed to build tree: %v", err)
		}

		// Try to verify leaf1 with leaf2's proof
		leaf1 := leafMap["key1"]
		wrongProof := merkleTree.Proofs[1] // leaf2's proof

		err = merkleTree.Verify(leaf1, wrongProof)
		if err == nil {
			t.Error("Expected verification to fail with wrong proof")
		}
	})

	t.Run("wrong root", func(t *testing.T) {
		// Create two different trees
		tree1 := CreateTree()
		tree1.AddLeaf("key1", "data1")
		tree1.AddLeaf("key2", "data2") // Add second leaf to meet minimum requirement
		merkleTree1, leafMap1, _ := tree1.Build()

		tree2 := CreateTree()
		tree2.AddLeaf("key1", "different_data")
		tree2.AddLeaf("key2", "data2") // Add second leaf to meet minimum requirement
		merkleTree2, _, _ := tree2.Build()

		// Try to verify leaf from tree1 with root from tree2
		leaf := leafMap1["key1"]
		proof := merkleTree1.Proofs[0]

		err := mt.Verify(leaf, proof, merkleTree2.Root, nil)
		if err == nil {
			t.Error("Expected verification to fail with wrong root")
		}
	})

	t.Run("modified leaf data", func(t *testing.T) {
		tree := CreateTree()
		tree.AddLeaf("key1", "original_data")
		tree.AddLeaf("key2", "other_data") // Add second leaf to meet minimum requirement

		merkleTree, _, err := tree.Build()
		if err != nil {
			t.Fatalf("Failed to build tree: %v", err)
		}

		// Create a new leaf with modified data
		modifiedLeaf := CreateLeaf("modified_data")

		// Try to verify modified leaf with original proof
		err = merkleTree.Verify(modifiedLeaf, merkleTree.Proofs[0])
		if err == nil {
			t.Error("Expected verification to fail with modified leaf data")
		}
	})
}
