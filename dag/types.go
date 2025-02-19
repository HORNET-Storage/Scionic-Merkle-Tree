package dag

import (
	"github.com/HORNET-Storage/scionic-merkletree/merkletree"
)

var ChunkSize = 2048 * 1024 // 2048 * 1024 bytes = 2 megabytes

type LeafType string

const (
	FileLeafType      LeafType = "file"
	ChunkLeafType     LeafType = "chunk"
	DirectoryLeafType LeafType = "directory"
)

type Dag struct {
	Root  string
	Leafs map[string]*DagLeaf
}

type DagBuilder struct {
	Leafs map[string]*DagLeaf
}

type DagLeaf struct {
	Hash              string
	ItemName          string
	Type              LeafType
	ContentHash       []byte
	Content           []byte
	ClassicMerkleRoot []byte
	CurrentLinkCount  int
	LatestLabel       string
	LeafCount         int
	Links             map[string]string
	ParentHash        string
	AdditionalData    map[string]string
	MerkleTree        *merkletree.MerkleTree
	LeafMap           map[string]merkletree.DataBlock
	Proofs            map[string]*ClassicTreeBranch
}

type DagLeafBuilder struct {
	ItemName string
	Label    int64
	LeafType LeafType
	Data     []byte
	Links    map[string]string
}

type ClassicTreeBranch struct {
	Leaf  string
	Proof *merkletree.Proof
}

type DagBranch struct {
	Leaf         *DagLeaf
	Path         []*DagLeaf
	MerkleProofs map[string]*ClassicTreeBranch
}

type testLeaf struct {
	data string
}

func (l *testLeaf) Serialize() ([]byte, error) {
	return []byte(l.data), nil
}

type MetaData struct {
	Deleted []string
}

func SetChunkSize(size int) {
	ChunkSize = size
}
