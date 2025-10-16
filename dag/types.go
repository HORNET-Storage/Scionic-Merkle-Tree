package dag

import (
	"io/fs"
	"sync"

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
	mu    sync.Mutex
}

type DagLeaf struct {
	// Hash is the CIDv1 content identifier for this leaf (label:hash format for non-root leaves)
	Hash string `json:"hash"`

	// ItemName is the filename or directory name for this leaf
	ItemName string `json:"item_name"`

	// Type specifies whether this is a file, directory, or chunk leaf
	Type LeafType `json:"type"`

	// ContentHash is the SHA-256 hash of the Content field (only present if Content is set)
	ContentHash []byte `json:"content_hash,omitempty"`

	// Content holds the actual file/chunk data
	Content []byte `json:"content,omitempty"`

	// ClassicMerkleRoot is the merkle root of all child leaf hashes (empty if no children)
	ClassicMerkleRoot []byte `json:"classic_merkle_root,omitempty"`

	// CurrentLinkCount is the total number of children this leaf has
	CurrentLinkCount int `json:"current_link_count"`

	// LatestLabel is the highest numeric label among all leaves (only set on root leaf)
	LatestLabel string `json:"latest_label,omitempty"`

	// LeafCount is the total number of leaves in the entire DAG (only set on root leaf)
	LeafCount int `json:"leaf_count,omitempty"`

	// ContentSize is the total size of actual content data across the entire DAG in bytes (only set on root leaf)
	ContentSize int64 `json:"content_size,omitempty"`

	// DagSize is the total serialized size of all leaves in the DAG in bytes (only set on root leaf)
	// Note: This value is approximate (within a few bytes) due to the two-pass serialization approach
	DagSize int64 `json:"dag_size,omitempty"`

	// Links maps child labels to their hashes (label -> "label:hash")
	Links map[string]string `json:"links,omitempty"`

	// ParentHash is the hash of the parent leaf (used during transmission, not stored in final DAG)
	ParentHash string `json:"parent_hash,omitempty"`

	// AdditionalData holds custom metadata key-value pairs added by LeafProcessor
	AdditionalData map[string]string `json:"additional_data,omitempty"`

	// MerkleTree is the computed merkle tree for this leaf's children (not serialized, computed on demand)
	MerkleTree *merkletree.MerkleTree `json:"-"`

	// LeafMap maps child hashes to their merkle tree data blocks (not serialized, computed on demand)
	LeafMap map[string]merkletree.DataBlock `json:"-"`

	// Proofs contains merkle proofs for children during partial DAG transmission (not stored in final DAG)
	Proofs map[string]*ClassicTreeBranch `json:"proofs,omitempty"`
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

type BatchedTransmissionPacket struct {
	Leaves        []*DagLeaf
	Relationships map[string]string
	Proofs        map[string]*ClassicTreeBranch
}

const DefaultBatchSize = 4 * 1024 * 1024 // 4MB default batch size

var BatchSize = DefaultBatchSize

func SetBatchSize(size int) {
	BatchSize = size
}

func DisableBatching() {
	SetBatchSize(-1)
}

func SetDefaultBatchSize() {
	SetBatchSize(DefaultBatchSize)
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

// DagBuilderConfig controls DAG building behavior
type DagBuilderConfig struct {
	// EnableParallel enables parallel processing of files and directories
	// Default: false (sequential processing for backward compatibility)
	EnableParallel bool

	// MaxWorkers controls the maximum number of concurrent goroutines when parallel processing
	// 0 = use runtime.NumCPU() (auto-detect based on available cores)
	// -1 = unlimited workers (not recommended, may overwhelm system)
	// >0 = use exactly this many workers
	// Only used when EnableParallel is true
	// Default: 0 (auto-detect)
	MaxWorkers int

	// TimestampRoot adds a timestamp to the root leaf's additional data
	// Default: false
	TimestampRoot bool

	// Processor is an optional function that generates custom metadata for each leaf
	// Default: nil (no custom metadata)
	Processor LeafProcessor
}

// DefaultConfig returns a config with sequential processing (backward compatible)
func DefaultConfig() *DagBuilderConfig {
	return &DagBuilderConfig{
		EnableParallel: false,
		MaxWorkers:     0,
		TimestampRoot:  false,
		Processor:      nil,
	}
}

// ParallelConfig returns a config with parallel processing enabled using all CPU cores
func ParallelConfig() *DagBuilderConfig {
	return &DagBuilderConfig{
		EnableParallel: true,
		MaxWorkers:     0, // Auto-detect
		TimestampRoot:  false,
		Processor:      nil,
	}
}

// ParallelConfigWithWorkers returns a config with a specific number of workers
func ParallelConfigWithWorkers(workers int) *DagBuilderConfig {
	return &DagBuilderConfig{
		EnableParallel: true,
		MaxWorkers:     workers,
		TimestampRoot:  false,
		Processor:      nil,
	}
}

// DiffType represents the type of difference between two DAG leaves
type DiffType string

const (
	// DiffTypeAdded indicates the leaf exists in the new DAG but not in the old DAG
	DiffTypeAdded DiffType = "added"

	// DiffTypeRemoved indicates the leaf exists in the old DAG but not in the new DAG
	DiffTypeRemoved DiffType = "removed"
)

// LeafDiff represents a difference between two DAGs
type LeafDiff struct {
	// Type indicates whether this leaf was added or removed
	Type DiffType `json:"type"`

	// BareHash is the hash without the label prefix
	BareHash string `json:"bare_hash"`

	// Leaf is the leaf from the DAG (from old DAG if removed, from new DAG if added)
	Leaf *DagLeaf `json:"leaf"`
}

// DagDiff represents the complete set of differences between two DAGs
type DagDiff struct {
	// Diffs maps bare hash to the difference for that leaf
	Diffs map[string]*LeafDiff `json:"diffs"`

	// Summary provides counts of each diff type
	Summary DiffSummary `json:"summary"`
}

// DiffSummary provides statistics about the differences
type DiffSummary struct {
	Added   int `json:"added"`
	Removed int `json:"removed"`
	Total   int `json:"total"`
}
