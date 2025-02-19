package dag

import (
	"encoding/json"

	merkle_tree "github.com/HORNET-Storage/scionic-merkletree/tree"
	cbor "github.com/fxamacker/cbor/v2"
)

// SerializableDag is a minimal version of Dag for efficient serialization
type SerializableDag struct {
	Root  string
	Leafs map[string]*SerializableDagLeaf
}

// SerializableDagLeaf is a minimal version of DagLeaf for efficient serialization
type SerializableDagLeaf struct {
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
	AdditionalData    map[string]string
}

// ToSerializable converts a Dag to its serializable form
func (dag *Dag) ToSerializable() *SerializableDag {
	serializable := &SerializableDag{
		Root:  dag.Root,
		Leafs: make(map[string]*SerializableDagLeaf),
	}

	for hash, leaf := range dag.Leafs {
		serializable.Leafs[hash] = leaf.ToSerializable()
	}

	return serializable
}

// FromSerializable reconstructs a full Dag from its serializable form
func FromSerializable(s *SerializableDag) *Dag {
	dag := &Dag{
		Root:  s.Root,
		Leafs: make(map[string]*DagLeaf),
	}

	// First pass: create all leaves
	for hash, sLeaf := range s.Leafs {
		dag.Leafs[hash] = &DagLeaf{
			Hash:              sLeaf.Hash,
			ItemName:          sLeaf.ItemName,
			Type:              sLeaf.Type,
			ContentHash:       sLeaf.ContentHash,
			Content:           sLeaf.Content,
			ClassicMerkleRoot: sLeaf.ClassicMerkleRoot,
			CurrentLinkCount:  sLeaf.CurrentLinkCount,
			Links:             make(map[string]string),
			AdditionalData:    make(map[string]string),
		}

		// Copy and sort links
		dag.Leafs[hash].Links = sortMapByKeys(sLeaf.Links)

		// Copy and sort additional data
		dag.Leafs[hash].AdditionalData = sortMapByKeys(sLeaf.AdditionalData)

		// Set root-specific fields
		if hash == s.Root {
			dag.Leafs[hash].LeafCount = sLeaf.LeafCount
			dag.Leafs[hash].LatestLabel = sLeaf.LatestLabel
		}
	}

	// Second pass: rebuild Merkle trees
	for _, leaf := range dag.Leafs {
		// Rebuild Merkle tree if leaf has multiple links
		if len(leaf.Links) > 1 {
			builder := merkle_tree.CreateTree()
			for _, link := range leaf.Links {
				builder.AddLeaf(GetLabel(link), link)
			}

			merkleTree, leafMap, err := builder.Build()
			if err == nil {
				leaf.MerkleTree = merkleTree
				leaf.LeafMap = leafMap
				leaf.ClassicMerkleRoot = merkleTree.Root
			}
		}
	}

	// Third pass: reconstruct parent hashes
	for hash, leaf := range dag.Leafs {
		for _, potential := range dag.Leafs {
			if potential.HasLink(hash) {
				leaf.ParentHash = potential.Hash
				break
			}
		}
	}

	return dag
}

// ToSerializable converts a DagLeaf to its serializable form
func (leaf *DagLeaf) ToSerializable() *SerializableDagLeaf {
	serializable := &SerializableDagLeaf{
		Hash:              leaf.Hash,
		ItemName:          leaf.ItemName,
		Type:              leaf.Type,
		ContentHash:       leaf.ContentHash,
		Content:           leaf.Content,
		ClassicMerkleRoot: leaf.ClassicMerkleRoot,
		CurrentLinkCount:  leaf.CurrentLinkCount,
		LatestLabel:       leaf.LatestLabel,
		LeafCount:         leaf.LeafCount,
		Links:             make(map[string]string),
		AdditionalData:    make(map[string]string),
	}

	// Copy and sort links
	serializable.Links = sortMapByKeys(leaf.Links)

	// Copy and sort additional data
	serializable.AdditionalData = sortMapByKeys(leaf.AdditionalData)

	return serializable
}

func (dag *Dag) ToCBOR() ([]byte, error) {
	serializable := dag.ToSerializable()
	return cbor.Marshal(serializable)
}

func (dag *Dag) ToJSON() ([]byte, error) {
	serializable := dag.ToSerializable()
	return json.MarshalIndent(serializable, "", "  ")
}

func FromCBOR(data []byte) (*Dag, error) {
	var serializable SerializableDag
	if err := cbor.Unmarshal(data, &serializable); err != nil {
		return nil, err
	}
	return FromSerializable(&serializable), nil
}

func FromJSON(data []byte) (*Dag, error) {
	var serializable SerializableDag
	if err := json.Unmarshal(data, &serializable); err != nil {
		return nil, err
	}
	return FromSerializable(&serializable), nil
}
