package dag

import (
	"bytes"
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
			hash := GetHash(link)
			builder.AddLeaf(hash, hash)
		}

		var err error
		merkleTree, leafMap, err = builder.Build()
		if err != nil {
			return nil, err
		}

		merkleRoot = merkleTree.Root
	} else if len(b.Links) == 1 {
		for _, link := range b.Links {
			merkleRoot = []byte(GetHash(link))
			break
		}
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
			hash := GetHash(link)
			builder.AddLeaf(hash, hash)
		}

		var err error
		merkleTree, leafMap, err = builder.Build()
		if err != nil {
			return nil, err
		}

		merkleRoot = merkleTree.Root
	} else if len(b.Links) == 1 {
		for _, link := range b.Links {
			merkleRoot = []byte(GetHash(link))
			break
		}
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

		// Find the hash that corresponds to this key
		var targetHash string
		var lookupHash string

		// First try using the key directly as a label
		if _, exists := leaf.Links[key]; exists {
			targetHash = leaf.Links[key]
			lookupHash = GetHash(targetHash)
		} else if HasLabel(key) {
			// If the key has a label, try finding it in the links
			label := GetLabel(key)
			if h, exists := leaf.Links[label]; exists {
				targetHash = h
				lookupHash = GetHash(targetHash)
			} else {
				// Otherwise, extract the hash from the key
				lookupHash = GetHash(key)
				targetHash = key
			}
		} else {
			// If the key is a hash, use it directly
			lookupHash = GetHash(key)
			targetHash = key
			// Try to find the full labeled version in links
			for _, h := range leaf.Links {
				if h == key || GetHash(h) == key {
					targetHash = h
					lookupHash = GetHash(h)
					break
				}
			}
		}

		if lookupHash == "" {
			return nil, fmt.Errorf("unable to find hash for key %s", key)
		}

		index, exists := leaf.MerkleTree.GetIndexForKey(lookupHash)
		if !exists {
			return nil, fmt.Errorf("unable to find index for hash %s", lookupHash)
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
	hash := GetHash(branch.Leaf)
	block := merkle_tree.CreateLeaf(hash)

	err := merkletree.Verify(block, branch.Proof, leaf.ClassicMerkleRoot, nil)
	if err != nil {
		return err
	}

	return nil
}

// VerifyChildrenAgainstMerkleRoot verifies that the ClassicMerkleRoot matches the actual children
// This should be called when we have all children present (len(Links) == CurrentLinkCount)
func (leaf *DagLeaf) VerifyChildrenAgainstMerkleRoot(dag *Dag) error {
	// Skip if no children
	if leaf.CurrentLinkCount == 0 {
		return nil
	}

	// Check if we have all children
	if len(leaf.Links) != leaf.CurrentLinkCount {
		return fmt.Errorf("cannot verify merkle root: have %d children but expected %d", len(leaf.Links), leaf.CurrentLinkCount)
	}

	// Case 1: Single child - ClassicMerkleRoot should be the child's hash
	if leaf.CurrentLinkCount == 1 {
		var childHash string
		for _, link := range leaf.Links {
			childHash = GetHash(link)
			break
		}

		// ClassicMerkleRoot should be the hash bytes of the single child
		expectedRoot := []byte(childHash)

		// Compare the roots
		if !bytes.Equal(leaf.ClassicMerkleRoot, expectedRoot) {
			return fmt.Errorf("single child merkle root mismatch: expected %x, got %x", expectedRoot, leaf.ClassicMerkleRoot)
		}
		return nil
	}

	// Case 2: Multiple children - rebuild the merkle tree and compare roots
	builder := merkle_tree.CreateTree()
	for _, link := range leaf.Links {
		hash := GetHash(link)
		builder.AddLeaf(hash, hash)
	}

	merkleTree, _, err := builder.Build()
	if err != nil {
		return fmt.Errorf("failed to rebuild merkle tree for verification: %w", err)
	}

	// Compare the rebuilt root with the stored root
	if !bytes.Equal(merkleTree.Root, leaf.ClassicMerkleRoot) {
		return fmt.Errorf("merkle root mismatch: rebuilt root %x does not match stored root %x", merkleTree.Root, leaf.ClassicMerkleRoot)
	}

	return nil
}

func (leaf *DagLeaf) VerifyLeaf() error {
	additionalData := sortMapByKeys(leaf.AdditionalData)

	merkleRoot := leaf.ClassicMerkleRoot
	if len(leaf.ClassicMerkleRoot) <= 0 {
		merkleRoot = []byte{}
	}

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
		MerkleRoot:       merkleRoot,
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

	if leaf.ClassicMerkleRoot == nil || len(leaf.ClassicMerkleRoot) <= 0 {
		leaf.ClassicMerkleRoot = []byte{}
	}

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
			var sortedLinks []struct {
				Label int
				Link  string
			}

			for label, link := range leaf.Links {
				labelNum, err := strconv.Atoi(label)
				if err != nil {
					return fmt.Errorf("invalid link label: %s", label)
				}

				sortedLinks = append(sortedLinks, struct {
					Label int
					Link  string
				}{
					Label: labelNum,
					Link:  link,
				})
			}

			sort.Slice(sortedLinks, func(i, j int) bool {
				return sortedLinks[i].Label < sortedLinks[j].Label
			})

			for _, item := range sortedLinks {
				childLeaf := dag.Leafs[item.Link]
				if childLeaf == nil {
					return fmt.Errorf("invalid link: %s", item.Link)
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
		ParentHash:        leaf.ParentHash,
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

	// MerkleTree and LeafMap are not deep-copied because they're regenerated when needed
	// But we preserve the ClassicMerkleRoot which is part of the leaf's identity

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
