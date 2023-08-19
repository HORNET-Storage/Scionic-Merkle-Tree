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
	Root   string
	Labels map[string]string
	Leafs  map[string]*DagLeaf
}

type DagBuilder struct {
	Labels map[string]string
	Leafs  map[string]*DagLeaf
}

type DagLeaf struct {
	Hash             string
	Name             string
	Type             LeafType
	Data             []byte
	MerkleRoot       []byte
	CurrentLinkCount int
	LeafCount        int
	LatestLabel      string
	Links            map[string]string
	ParentHash       string
}

type DagLeafBuilder struct {
	Name     string
	Label    int64
	LeafType LeafType
	Data     []byte
	Links    map[string]string
}

type ClassicTreeBranch struct {
	Leaf  *merkletree.DataBlock
	Proof *merkletree.Proof
}

func SetChunkSize(size int) {
	ChunkSize = size
}
