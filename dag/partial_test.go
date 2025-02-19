package dag

import (
	"strconv"
	"testing"

	"github.com/HORNET-Storage/scionic-merkletree/merkletree"
)

func TestGetPartial(t *testing.T) {
	// Create a test DAG with multiple levels and branches
	dagBuilder := CreateDagBuilder()

	// Create some test leaves
	for i := 1; i <= 10; i++ {
		leaf := &DagLeaf{
			Hash:     strconv.Itoa(i) + ":test" + strconv.Itoa(i),
			ItemName: "test" + strconv.Itoa(i),
			Type:     FileLeafType,
			Content:  []byte("test content " + strconv.Itoa(i)),
			Links:    make(map[string]string),
		}
		dagBuilder.AddLeaf(leaf, nil)
	}

	// Create a parent with merkle tree (multiple children)
	parent1 := &DagLeaf{
		Hash:     "parent1",
		ItemName: "parent1",
		Type:     DirectoryLeafType,
		Links:    make(map[string]string),
	}

	// Add first 5 leaves to parent1 and build merkle tree
	leafMap1 := make(map[string]merkletree.DataBlock)
	for i := 1; i <= 5; i++ {
		label := strconv.Itoa(i)
		hash := label + ":test" + label
		parent1.Links[label] = hash
		leafMap1[label] = &testLeaf{data: hash}
	}
	merkleTree1, err := merkletree.New(nil, leafMap1)
	if err != nil {
		t.Fatalf("Failed to build merkle tree: %v", err)
	}
	parent1.ClassicMerkleRoot = merkleTree1.Root
	parent1.MerkleTree = merkleTree1
	parent1.LeafMap = leafMap1
	dagBuilder.AddLeaf(parent1, nil)

	// Create another parent with merkle tree
	parent2 := &DagLeaf{
		Hash:     "parent2",
		ItemName: "parent2",
		Type:     DirectoryLeafType,
		Links:    make(map[string]string),
	}

	// Add remaining leaves to parent2 and build merkle tree
	leafMap2 := make(map[string]merkletree.DataBlock)
	for i := 6; i <= 10; i++ {
		label := strconv.Itoa(i)
		hash := label + ":test" + label
		parent2.Links[label] = hash
		leafMap2[label] = &testLeaf{data: hash}
	}
	merkleTree2, err := merkletree.New(nil, leafMap2)
	if err != nil {
		t.Fatalf("Failed to build merkle tree: %v", err)
	}
	parent2.ClassicMerkleRoot = merkleTree2.Root
	parent2.MerkleTree = merkleTree2
	parent2.LeafMap = leafMap2
	dagBuilder.AddLeaf(parent2, nil)

	// Create root that links both parents
	root := &DagLeaf{
		Hash:     "root",
		ItemName: "root",
		Type:     DirectoryLeafType,
		Links:    make(map[string]string),
	}
	root.Links["11"] = parent1.Hash
	root.Links["12"] = parent2.Hash

	// Build merkle tree for root
	rootLeafMap := make(map[string]merkletree.DataBlock)
	rootLeafMap["11"] = &testLeaf{data: parent1.Hash}
	rootLeafMap["12"] = &testLeaf{data: parent2.Hash}
	rootMerkleTree, err := merkletree.New(nil, rootLeafMap)
	if err != nil {
		t.Fatalf("Failed to build root merkle tree: %v", err)
	}
	root.ClassicMerkleRoot = rootMerkleTree.Root
	root.MerkleTree = rootMerkleTree
	root.LeafMap = rootLeafMap

	dagBuilder.AddLeaf(root, nil)

	// Build the DAG
	dag := dagBuilder.BuildDag(root.Hash)

	// Test getting a partial DAG
	partial, err := dag.GetPartial(3, 7)
	if err != nil {
		t.Fatalf("Failed to get partial DAG: %v", err)
	}

	// Verify the partial DAG structure
	if partial.Root != dag.Root {
		t.Error("Partial DAG has incorrect root")
	}

	// Should contain:
	// - Leaves 3,4,5 (from parent1)
	// - Leaves 6,7 (from parent2)
	// - parent1 and parent2 (with their merkle proofs)
	// - root
	expectedLeaves := []string{"3", "4", "5", "6", "7"}
	for _, label := range expectedLeaves {
		found := false
		for _, leaf := range partial.Leafs {
			if GetLabel(leaf.Hash) == label {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Partial DAG missing leaf with label %s", label)
		}
	}

	// Verify parent nodes are present
	if _, exists := partial.Leafs["parent1"]; !exists {
		t.Error("Partial DAG missing parent1")
	}
	if _, exists := partial.Leafs["parent2"]; !exists {
		t.Error("Partial DAG missing parent2")
	}

	// Verify merkle proofs
	err = partial.VerifyPartial()
	if err != nil {
		t.Errorf("Partial DAG verification failed: %v", err)
	}

	// Test invalid range
	_, err = dag.GetPartial(15, 20)
	if err != nil {
		t.Error("GetPartial should handle invalid range gracefully")
	}
}

func TestGetPartialSingleLeaf(t *testing.T) {
	// Create a simple DAG with one leaf
	builder := CreateDagBuilder()

	leaf := &DagLeaf{
		Hash:     "1:test1",
		ItemName: "test1",
		Type:     FileLeafType,
		Content:  []byte("test content"),
		Links:    make(map[string]string),
	}
	builder.AddLeaf(leaf, nil)

	root := &DagLeaf{
		Hash:     "root",
		ItemName: "root",
		Type:     DirectoryLeafType,
		Links:    make(map[string]string),
	}
	root.Links["1"] = leaf.Hash
	builder.AddLeaf(root, nil)

	dag := builder.BuildDag(root.Hash)

	// Test getting partial DAG with single leaf
	partial, err := dag.GetPartial(1, 1)
	if err != nil {
		t.Fatalf("Failed to get partial DAG: %v", err)
	}

	// Verify structure
	if partial.Root != dag.Root {
		t.Error("Partial DAG has incorrect root")
	}

	if _, exists := partial.Leafs["1:test1"]; !exists {
		t.Error("Partial DAG missing leaf")
	}

	if _, exists := partial.Leafs["root"]; !exists {
		t.Error("Partial DAG missing root")
	}

	// Verify the partial DAG
	err = partial.VerifyPartial()
	if err != nil {
		t.Errorf("Partial DAG verification failed: %v", err)
	}
}
