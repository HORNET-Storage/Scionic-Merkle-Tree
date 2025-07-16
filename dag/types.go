package dag

import (
	"io/fs"

	"github.com/HORNET-Storage/Scionic-Merkle-Tree/merkletree"
)

const DefaultChunkSize = 2048 * 1024 // 2048 * 1024 bytes = 2 megabytes

var ChunkSize = DefaultChunkSize

type LeafType string

const (
	FileLeafType      LeafType = "file"
	ChunkLeafType     LeafType = "chunk"
	DirectoryLeafType LeafType = "directory"
)

// LeafProcessor is a function that generates metadata for a leaf
// path: The full path to the file/directory
// relPath: The relative path within the DAG
// entry: The file/directory entry information
// isRoot: Whether this is the root leaf
// leafType: The type of leaf (file, directory, chunk)
// Returns additional metadata to be added to the leaf
type LeafProcessor func(path string, relPath string, entry fs.DirEntry, isRoot bool, leafType LeafType) map[string]string

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

type TransmissionPacket struct {
	Leaf       *DagLeaf
	ParentHash string
	Proofs     map[string]*ClassicTreeBranch
}

func SetChunkSize(size int) {
	ChunkSize = size
}

func DisableChunking() {
	SetChunkSize(-1)
}

func SetDefaultChunkSize() {
	SetChunkSize(DefaultChunkSize)
}
