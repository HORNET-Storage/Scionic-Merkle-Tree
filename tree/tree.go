package tree

import (
	"fmt"

	//mt "github.com/txaty/go-merkletree"
	mt "github.com/HORNET-Storage/scionic-merkletree/merkletree"
)

type TreeContent struct {
	leafs map[string]mt.DataBlock
}

type Leaf struct {
	data string
}

func (b *Leaf) Serialize() ([]byte, error) {
	return []byte(b.data), nil
}

func CreateTree() *TreeContent {
	tree := TreeContent{
		map[string]mt.DataBlock{},
	}

	return &tree
}

func CreateLeaf(data string) *Leaf {
	return &Leaf{data}
}

func (tc *TreeContent) AddLeaf(key string, data string) {
	leaf := CreateLeaf(data)

	tc.leafs[key] = leaf
}

func (tc *TreeContent) Build() (*mt.MerkleTree, map[string]mt.DataBlock, error) {
	tree, err := mt.New(nil, tc.leafs)
	if err != nil {
		return nil, nil, err
	}

	return tree, tc.leafs, err
}

func VerifyTree(tree *mt.MerkleTree, leafs []mt.DataBlock) bool {
	if len(tree.Proofs) != len(leafs) {
		return false
	}

	for i := 0; i < len(leafs); i++ {
		err := tree.Verify(leafs[i], tree.Proofs[i])
		if err != nil {
			fmt.Printf("Verification failed for leaf %d: %v\n", i, err)
			return false
		}
	}

	return true
}

func VerifyRoot(root []byte, proofs []*mt.Proof, leafs []mt.DataBlock) bool {
	if len(proofs) != len(leafs) {
		return false
	}

	for i := 0; i < len(leafs); i++ {
		// if hashFunc is nil, use SHA256 by default
		err := mt.Verify(leafs[i], proofs[i], root, nil)
		if err != nil {
			fmt.Printf("Verification failed for leaf %d: %v\n", i, err)
			return false
		}
	}

	return true
}
