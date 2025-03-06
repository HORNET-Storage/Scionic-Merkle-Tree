package dag

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/HORNET-Storage/Scionic-Merkle-Tree/merkletree"

	cbor "github.com/fxamacker/cbor/v2"

	merkle_tree "github.com/HORNET-Storage/Scionic-Merkle-Tree/tree"

	"github.com/ipfs/go-cid"
	mc "github.com/multiformats/go-multicodec"
	mh "github.com/multiformats/go-multihash"
)

func CreateDagLeafBuilder(name string) *DagLeafBuilder {
	builder := &DagLeafBuilder{
		ItemName: name,
		Links:    map[string]string{},
	}

	return builder
}

func (b *DagLeafBuilder) SetType(leafType LeafType) {
	b.LeafType = leafType
}

func (b *DagLeafBuilder) SetData(data []byte) {
	b.Data = data
}

func (b *DagLeafBuilder) AddLink(label string, hash string) {
	b.Links[label] = label + ":" + hash
}

func (b *DagBuilder) GetLatestLabel() string {
	var result string = "0"
	var latestLabel int64 = 0
	for hash := range b.Leafs {
		label := GetLabel(hash)

		if label == "" {
			fmt.Println("Failed to find label in hash")
		}

		parsed, err := strconv.ParseInt(label, 10, 64)
		if err != nil {
			fmt.Println("Failed to parse label")
		}

		if parsed > latestLabel {
			latestLabel = parsed
			result = label
		}
	}

	return result
}

func (b *DagBuilder) GetNextAvailableLabel() string {
	latestLabel := b.GetLatestLabel()

	number, err := strconv.ParseInt(latestLabel, 10, 64)
	if err != nil {
		fmt.Println("Failed to parse label")
	}

	nextLabel := strconv.FormatInt(number+1, 10)

	return nextLabel
}

func (b *DagLeafBuilder) BuildLeaf(additionalData map[string]string) (*DagLeaf, error) {
	if b.LeafType == "" {
		err := fmt.Errorf("leaf must have a type defined")
		return nil, err
	}

	merkleRoot := []byte{}
	var merkleTree *merkletree.MerkleTree
	var leafMap map[string]merkletree.DataBlock

	if len(b.Links) > 1 {
		builder := merkle_tree.CreateTree()
		for _, link := range b.Links {
			builder.AddLeaf(GetLabel(link), link)
		}

		var err error
		merkleTree, leafMap, err = builder.Build()
		if err != nil {
			return nil, err
		}

		merkleRoot = merkleTree.Root
	}

	additionalData = sortMapByKeys(additionalData)

	leafData := struct {
		ItemName         string
		Type             LeafType
		MerkleRoot       []byte
		CurrentLinkCount int
		ContentHash      []byte
		AdditionalData   []keyValue
	}{
		ItemName:         b.ItemName,
		Type:             b.LeafType,
		MerkleRoot:       merkleRoot,
		CurrentLinkCount: len(b.Links),
		ContentHash:      nil,
		AdditionalData:   sortMapForVerification(additionalData),
	}

	if b.Data != nil {
		hash := sha256.Sum256(b.Data)
		leafData.ContentHash = hash[:]
	}

	serializedLeafData, err := cbor.Marshal(leafData)
	if err != nil {
		return nil, err
	}

	pref := cid.Prefix{
		Version:  1,
		Codec:    uint64(mc.Cbor),
		MhType:   mh.SHA2_256,
		MhLength: -1,
	}

	c, err := pref.Sum(serializedLeafData)
	if err != nil {
		return nil, err
	}

	sortedLinks := sortMapByKeys(b.Links)
	leaf := &DagLeaf{
		Hash:              c.String(),
		ItemName:          b.ItemName,
		Type:              b.LeafType,
		ClassicMerkleRoot: merkleRoot,
		CurrentLinkCount:  len(b.Links),
		Content:           b.Data,
		ContentHash:       leafData.ContentHash,
		Links:             sortedLinks,
		AdditionalData:    additionalData,
		MerkleTree:        merkleTree,
		LeafMap:           leafMap,
	}

	return leaf, nil
}

func (b *DagLeafBuilder) BuildRootLeaf(dag *DagBuilder, additionalData map[string]string) (*DagLeaf, error) {
	if b.LeafType == "" {
		err := fmt.Errorf("leaf must have a type defined")
		return nil, err
	}

	merkleRoot := []byte{}
	var merkleTree *merkletree.MerkleTree
	var leafMap map[string]merkletree.DataBlock

	if len(b.Links) > 1 {
		builder := merkle_tree.CreateTree()
		for _, link := range b.Links {
			builder.AddLeaf(GetLabel(link), link)
		}

		var err error
		merkleTree, leafMap, err = builder.Build()
		if err != nil {
			return nil, err
		}

		merkleRoot = merkleTree.Root
	}

	latestLabel := dag.GetLatestLabel()

	additionalData = sortMapByKeys(additionalData)

	leafData := struct {
		ItemName         string
		Type             LeafType
		MerkleRoot       []byte
		CurrentLinkCount int
		LatestLabel      string
		LeafCount        int
		ContentHash      []byte
		AdditionalData   []keyValue
	}{
		ItemName:         b.ItemName,
		Type:             b.LeafType,
		MerkleRoot:       merkleRoot,
		CurrentLinkCount: len(b.Links),
		LatestLabel:      latestLabel,
		LeafCount:        len(dag.Leafs),
		ContentHash:      nil,
		AdditionalData:   sortMapForVerification(additionalData),
	}

	if b.Data != nil {
		hash := sha256.Sum256(b.Data)
		leafData.ContentHash = hash[:]
	}

	serializedLeafData, err := cbor.Marshal(leafData)
	if err != nil {
		return nil, err
	}

	pref := cid.Prefix{
		Version:  1,
		Codec:    uint64(mc.Cbor),
		MhType:   mh.SHA2_256,
		MhLength: -1,
	}

	c, err := pref.Sum(serializedLeafData)
	if err != nil {
		return nil, err
	}

	sortedLinks := sortMapByKeys(b.Links)
	leaf := &DagLeaf{
		Hash:              c.String(),
		ItemName:          b.ItemName,
		Type:              b.LeafType,
		ClassicMerkleRoot: merkleRoot,
		CurrentLinkCount:  len(b.Links),
		LatestLabel:       latestLabel,
		LeafCount:         len(dag.Leafs),
		Content:           b.Data,
		ContentHash:       leafData.ContentHash,
		Links:             sortedLinks,
		AdditionalData:    additionalData,
		MerkleTree:        merkleTree,
		LeafMap:           leafMap,
	}

	return leaf, nil
}

func (leaf *DagLeaf) GetIndexForKey(key string) (int, bool) {
	if leaf.MerkleTree == nil {
		return -1, false
	}

	index, exists := leaf.MerkleTree.GetIndexForKey(key)
	return index, exists
}

func (leaf *DagLeaf) GetBranch(key string) (*ClassicTreeBranch, error) {
	if len(leaf.Links) > 1 {
		if leaf.MerkleTree == nil {
			return nil, fmt.Errorf("merkle tree not built for leaf")
		}

		// Find the label that maps to this hash
		var label string
		targetHash := key

		// First try using the key directly as a label
		if _, exists := leaf.Links[key]; exists {
			label = key
			targetHash = leaf.Links[key]
		} else if HasLabel(key) {
			// If the key has a label, try finding it in the links
			label = GetLabel(key)
			if h, exists := leaf.Links[label]; exists {
				targetHash = h
			}
		} else {
			// If the key is a hash, find its label
			for l, h := range leaf.Links {
				if h == key || GetHash(h) == key {
					label = l
					targetHash = h
					break
				}
			}
		}

		if label == "" {
			return nil, fmt.Errorf("unable to find label for key %s", key)
		}

		index, exists := leaf.MerkleTree.GetIndexForKey(label)
		if !exists {
			return nil, fmt.Errorf("unable to find index for key %s", label)
		}

		branch := &ClassicTreeBranch{
			Leaf:  targetHash,
			Proof: leaf.MerkleTree.Proofs[index],
		}

		return branch, nil
	}
	return nil, nil
}

func (leaf *DagLeaf) VerifyBranch(branch *ClassicTreeBranch) error {
	block := merkle_tree.CreateLeaf(branch.Leaf)

	err := merkletree.Verify(block, branch.Proof, leaf.ClassicMerkleRoot, nil)
	if err != nil {
		return err
	}

	return nil
}

func (leaf *DagLeaf) VerifyLeaf() error {
	additionalData := sortMapByKeys(leaf.AdditionalData)

	leafData := struct {
		ItemName         string
		Type             LeafType
		MerkleRoot       []byte
		CurrentLinkCount int
		ContentHash      []byte
		AdditionalData   []keyValue
	}{
		ItemName:         leaf.ItemName,
		Type:             leaf.Type,
		MerkleRoot:       leaf.ClassicMerkleRoot,
		CurrentLinkCount: leaf.CurrentLinkCount,
		ContentHash:      leaf.ContentHash,
		AdditionalData:   sortMapForVerification(additionalData),
	}

	serializedLeafData, err := cbor.Marshal(leafData)
	if err != nil {
		return err
	}

	pref := cid.Prefix{
		Version:  1,
		Codec:    uint64(mc.Cbor),
		MhType:   mh.SHA2_256,
		MhLength: -1,
	}

	c, err := pref.Sum(serializedLeafData)
	if err != nil {
		return err
	}

	currentCid, err := cid.Decode(GetHash(leaf.Hash))
	if err != nil {
		return err
	}

	success := c.Equals(currentCid)
	if !success {
		return fmt.Errorf("leaf failed to verify")
	}

	return nil
}

func (leaf *DagLeaf) VerifyRootLeaf() error {
	additionalData := sortMapByKeys(leaf.AdditionalData)

	leafData := struct {
		ItemName         string
		Type             LeafType
		MerkleRoot       []byte
		CurrentLinkCount int
		LatestLabel      string
		LeafCount        int
		ContentHash      []byte
		AdditionalData   []keyValue
	}{
		ItemName:         leaf.ItemName,
		Type:             leaf.Type,
		MerkleRoot:       leaf.ClassicMerkleRoot,
		CurrentLinkCount: leaf.CurrentLinkCount,
		LatestLabel:      leaf.LatestLabel,
		LeafCount:        leaf.LeafCount,
		ContentHash:      leaf.ContentHash,
		AdditionalData:   sortMapForVerification(additionalData),
	}

	serializedLeafData, err := cbor.Marshal(leafData)
	if err != nil {
		return err
	}

	pref := cid.Prefix{
		Version:  1,
		Codec:    uint64(mc.Cbor),
		MhType:   mh.SHA2_256,
		MhLength: -1,
	}

	c, err := pref.Sum(serializedLeafData)
	if err != nil {
		return err
	}

	currentCid, err := cid.Decode(GetHash(leaf.Hash))
	if err != nil {
		return err
	}

	success := c.Equals(currentCid)
	if !success {
		return fmt.Errorf("leaf failed to verify")
	}

	return nil
}

func (leaf *DagLeaf) CreateDirectoryLeaf(path string, dag *Dag) error {
	switch leaf.Type {
	case DirectoryLeafType:
		_ = os.Mkdir(path, os.ModePerm)

		for _, link := range leaf.Links {
			childLeaf := dag.Leafs[link]
			if childLeaf == nil {
				return fmt.Errorf("invalid link: %s", link)
			}

			childPath := filepath.Join(path, childLeaf.ItemName)
			err := childLeaf.CreateDirectoryLeaf(childPath, dag)
			if err != nil {
				return err
			}
		}

	case FileLeafType:
		var content []byte

		if len(leaf.Links) > 0 {
			for _, link := range leaf.Links {
				childLeaf := dag.Leafs[link]
				if childLeaf == nil {
					return fmt.Errorf("invalid link: %s", link)
				}

				content = append(content, childLeaf.Content...)
			}
		} else {
			content = leaf.Content
		}

		err := os.WriteFile(path, content, os.ModePerm)
		if err != nil {
			return err
		}
	}

	return nil
}

func (leaf *DagLeaf) HasLink(hash string) bool {
	for _, link := range leaf.Links {
		if HasLabel(hash) {
			if HasLabel(link) {
				if link == hash {
					return true
				}
			} else {
				if link == GetHash(hash) {
					return true
				}
			}
		} else {
			if HasLabel(link) {
				if GetHash(link) == hash {
					return true
				}
			} else {
				if GetHash(link) == GetHash(hash) {
					return true
				}
			}
		}
	}

	return false
}

func (leaf *DagLeaf) AddLink(hash string) {
	label := GetLabel(hash)

	if label == "" {
		fmt.Println("This hash does not have a label")
	}

	leaf.Links[label] = hash
}

func (leaf *DagLeaf) Clone() *DagLeaf {
	cloned := &DagLeaf{
		Hash:              leaf.Hash,
		ItemName:          leaf.ItemName,
		Type:              leaf.Type,
		Content:           leaf.Content,
		ContentHash:       leaf.ContentHash,
		ClassicMerkleRoot: leaf.ClassicMerkleRoot,
		CurrentLinkCount:  leaf.CurrentLinkCount,
		LatestLabel:       leaf.LatestLabel,
		LeafCount:         leaf.LeafCount,
		Links:             make(map[string]string),
		AdditionalData:    make(map[string]string),
		Proofs:            make(map[string]*ClassicTreeBranch),
	}

	// Deep copy maps
	for k, v := range leaf.Links {
		cloned.Links[k] = v
	}
	for k, v := range leaf.AdditionalData {
		cloned.AdditionalData[k] = v
	}
	if leaf.Proofs != nil {
		for k, v := range leaf.Proofs {
			cloned.Proofs[k] = v
		}
	}

	// Copy root-specific fields if this is the root leaf
	if leaf.Hash == leaf.ParentHash || leaf.Hash == GetHash(leaf.Hash) {
		cloned.LatestLabel = leaf.LatestLabel
		cloned.LeafCount = leaf.LeafCount
		cloned.ParentHash = cloned.Hash // Root is its own parent
	} else {
		cloned.ParentHash = leaf.ParentHash
	}

	// If leaf has multiple children according to CurrentLinkCount,
	// we need to handle its merkle tree state
	if leaf.CurrentLinkCount > 1 {
		if len(leaf.Links) > 1 {
			// Build merkle tree with current links
			builder := merkle_tree.CreateTree()
			for l, h := range leaf.Links {
				builder.AddLeaf(l, h)
			}
			merkleTree, leafMap, err := builder.Build()
			if err == nil {
				cloned.MerkleTree = merkleTree
				cloned.LeafMap = leafMap
				cloned.ClassicMerkleRoot = merkleTree.Root
			}
		} else {
			// Clear merkle tree if we don't have enough links to rebuild it
			cloned.MerkleTree = nil
			cloned.LeafMap = nil
			// But keep ClassicMerkleRoot as it's part of the leaf's identity
			cloned.ClassicMerkleRoot = leaf.ClassicMerkleRoot
		}
	}

	return cloned
}

func (leaf *DagLeaf) SetLabel(label string) {
	leaf.Hash = label + ":" + leaf.Hash
}

func HasLabel(hash string) bool {
	if GetLabel(hash) != "" {
		return true
	} else {
		return false
	}
}

func GetHash(hash string) string {
	parts := strings.Split(hash, ":")

	if len(parts) != 2 {
		return hash
	}

	return parts[1]
}

func GetLabel(hash string) string {
	parts := strings.Split(hash, ":")
	if len(parts) != 2 {
		return ""
	}

	return parts[0]
}

func sortMapByKeys(inputMap map[string]string) map[string]string {
	if inputMap == nil {
		return map[string]string{}
	}

	if len(inputMap) <= 0 {
		return map[string]string{}
	}

	keys := make([]string, 0, len(inputMap))

	for key := range inputMap {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	sortedMap := make(map[string]string)
	for _, key := range keys {
		sortedMap[key] = inputMap[key]
	}

	return sortedMap
}

type keyValue struct {
	Key   string
	Value string
}

func sortMapForVerification(inputMap map[string]string) []keyValue {
	if inputMap == nil {
		return nil
	}

	keys := make([]string, 0, len(inputMap))
	for key := range inputMap {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	sortedPairs := make([]keyValue, 0, len(keys))
	for _, key := range keys {
		sortedPairs = append(sortedPairs, keyValue{Key: key, Value: inputMap[key]})
	}

	return sortedPairs
}
