package dag

import (
	"encoding/json"

	merkle_tree "github.com/HORNET-Storage/Scionic-Merkle-Tree/tree"
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
	StoredProofs      map[string]*ClassicTreeBranch `json:"stored_proofs,omitempty" cbor:"stored_proofs,omitempty"`
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

// FromSerializable reconstructs a Dag from its serializable form
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
			Proofs:            make(map[string]*ClassicTreeBranch),
		}

		// Copy and sort links
		dag.Leafs[hash].Links = sortMapByKeys(sLeaf.Links)

		// Copy and sort additional data
		dag.Leafs[hash].AdditionalData = sortMapByKeys(sLeaf.AdditionalData)

		// Copy stored proofs
		if sLeaf.StoredProofs != nil {
			for k, v := range sLeaf.StoredProofs {
				dag.Leafs[hash].Proofs[k] = v
			}
		}

		// Set root-specific fields
		if hash == s.Root {
			dag.Leafs[hash].LeafCount = sLeaf.LeafCount
			dag.Leafs[hash].LatestLabel = sLeaf.LatestLabel
		}
	}

	// Check if this is a partial DAG
	isPartial := false
	for _, leaf := range dag.Leafs {
		if len(leaf.Links) < leaf.CurrentLinkCount {
			isPartial = true
			break
		}
	}

	// For full DAGs, rebuild Merkle trees
	// For partial DAGs, preserve the existing Merkle roots
	if !isPartial {
		// Second pass: rebuild Merkle trees for full DAGs
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
		StoredProofs:      make(map[string]*ClassicTreeBranch),
	}

	// Copy and sort links
	serializable.Links = sortMapByKeys(leaf.Links)

	// Copy and sort additional data
	serializable.AdditionalData = sortMapByKeys(leaf.AdditionalData)

	// Copy stored proofs
	if leaf.Proofs != nil {
		for k, v := range leaf.Proofs {
			serializable.StoredProofs[k] = v
		}
	}

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
