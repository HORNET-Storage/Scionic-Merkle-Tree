package dag

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sort"

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
		Links:    []string{},
	}

	return builder
}

func (b *DagLeafBuilder) SetType(leafType LeafType) {
	b.LeafType = leafType
}

func (b *DagLeafBuilder) SetData(data []byte) {
	b.Data = data
}

func (b *DagLeafBuilder) AddLink(hash string) {
	b.Links = append(b.Links, hash)
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
			builder.AddLeaf(link, link)
		}

		var err error
		merkleTree, leafMap, err = builder.Build()
		if err != nil {
			return nil, err
		}

		merkleRoot = merkleTree.Root
	} else if len(b.Links) == 1 {
		for _, link := range b.Links {
			merkleRoot = []byte(link)
			break
		}
	}

	additionalData = SortMapByKeys(additionalData)

	leafData := struct {
		ItemName         string
		Type             LeafType
		MerkleRoot       []byte
		CurrentLinkCount int
		ContentHash      []byte
		AdditionalData   []KeyValue
	}{
		ItemName:         b.ItemName,
		Type:             b.LeafType,
		MerkleRoot:       merkleRoot,
		CurrentLinkCount: len(b.Links),
		ContentHash:      nil,
		AdditionalData:   SortMapForVerification(additionalData),
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

	// Sort links for consistency (but only for directories, not files with chunks)
	// For files, chunk order must be preserved for correct reconstruction
	sortedLinks := b.Links
	if b.LeafType == DirectoryLeafType {
		sortedLinks = make([]string, len(b.Links))
		copy(sortedLinks, b.Links)
		sort.Strings(sortedLinks)
	}

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
			builder.AddLeaf(link, link)
		}

		var err error
		merkleTree, leafMap, err = builder.Build()
		if err != nil {
			return nil, err
		}

		merkleRoot = merkleTree.Root
	} else if len(b.Links) == 1 {
		for _, link := range b.Links {
			merkleRoot = []byte(link)
			break
		}
	}

	dagObj := &Dag{Leafs: dag.Leafs}
	contentSize := CalculateTotalContentSize(dagObj)
	if b.Data != nil {
		contentSize += int64(len(b.Data))
	}

	childrenDagSize, err := CalculateTotalDagSize(dagObj)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate dag size: %w", err)
	}

	tempLeafData := struct {
		ItemName         string
		Type             LeafType
		MerkleRoot       []byte
		CurrentLinkCount int
		LeafCount        int
		ContentSize      int64
		DagSize          int64
		ContentHash      []byte
		AdditionalData   []KeyValue
	}{
		ItemName:         b.ItemName,
		Type:             b.LeafType,
		MerkleRoot:       merkleRoot,
		CurrentLinkCount: len(b.Links),
		LeafCount:        len(dag.Leafs) + 1,
		ContentSize:      contentSize,
		DagSize:          0,
		ContentHash:      nil,
		AdditionalData:   SortMapForVerification(additionalData),
	}

	if b.Data != nil {
		hash := sha256.Sum256(b.Data)
		tempLeafData.ContentHash = hash[:]
	}

	tempSerialized, err := cbor.Marshal(tempLeafData)
	if err != nil {
		return nil, err
	}
	rootLeafSize := int64(len(tempSerialized))

	finalDagSize := childrenDagSize + rootLeafSize

	additionalData = SortMapByKeys(additionalData)

	leafData := struct {
		ItemName         string
		Type             LeafType
		MerkleRoot       []byte
		CurrentLinkCount int
		LeafCount        int
		ContentSize      int64
		DagSize          int64
		ContentHash      []byte
		AdditionalData   []KeyValue
	}{
		ItemName:         b.ItemName,
		Type:             b.LeafType,
		MerkleRoot:       merkleRoot,
		CurrentLinkCount: len(b.Links),
		LeafCount:        len(dag.Leafs) + 1,
		ContentSize:      contentSize,
		DagSize:          finalDagSize,
		ContentHash:      tempLeafData.ContentHash,
		AdditionalData:   SortMapForVerification(additionalData),
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

	// Sort links for consistency (but only for directories, not files with chunks)
	// For files, chunk order must be preserved for correct reconstruction
	sortedLinks := b.Links
	if b.LeafType == DirectoryLeafType {
		sortedLinks = make([]string, len(b.Links))
		copy(sortedLinks, b.Links)
		sort.Strings(sortedLinks)
	}

	leaf := &DagLeaf{
		Hash:              c.String(),
		ItemName:          b.ItemName,
		Type:              b.LeafType,
		ClassicMerkleRoot: merkleRoot,
		CurrentLinkCount:  len(b.Links),
		LeafCount:         len(dag.Leafs) + 1,
		ContentSize:       contentSize,
		DagSize:           finalDagSize,
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

		index, exists := leaf.MerkleTree.GetIndexForKey(key)
		if !exists {
			return nil, fmt.Errorf("unable to find index for hash %s", key)
		}

		branch := &ClassicTreeBranch{
			Leaf:  key,
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

// VerifyChildrenAgainstMerkleRoot checks ClassicMerkleRoot against actual children
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
			childHash = link
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
		hash := link
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
	additionalData := SortMapByKeys(leaf.AdditionalData)

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
		AdditionalData   []KeyValue
	}{
		ItemName:         leaf.ItemName,
		Type:             leaf.Type,
		MerkleRoot:       merkleRoot,
		CurrentLinkCount: leaf.CurrentLinkCount,
		ContentHash:      leaf.ContentHash,
		AdditionalData:   SortMapForVerification(additionalData),
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

	currentCid, err := cid.Decode(leaf.Hash)
	if err != nil {
		return err
	}

	success := c.Equals(currentCid)
	if !success {
		return fmt.Errorf("leaf failed to verify")
	}

	return nil
}

func (leaf *DagLeaf) VerifyRootLeaf(dag *Dag) error {
	additionalData := SortMapByKeys(leaf.AdditionalData)

	if len(leaf.ClassicMerkleRoot) <= 0 {
		leaf.ClassicMerkleRoot = []byte{}
	}

	var calculatedContentSize int64
	var calculatedDagSize int64

	isFullDag := dag != nil &&
		len(dag.Leafs) == leaf.LeafCount &&
		(leaf.ContentSize != 0 || leaf.DagSize != 0 || leaf.LeafCount == 1)

	// If any leaf has pruned links, this is a partial DAG
	if isFullDag && dag != nil {
		for _, l := range dag.Leafs {
			if len(l.Links) < l.CurrentLinkCount {
				isFullDag = false
				break
			}
		}
	}

	if isFullDag {
		for _, dagLeaf := range dag.Leafs {
			if dagLeaf.Content != nil {
				calculatedContentSize += int64(len(dagLeaf.Content))
			}
		}

		rootHash := leaf.Hash
		var childrenDagSize int64
		for _, dagLeaf := range dag.Leafs {
			if dagLeaf.Hash == rootHash {
				continue
			}

			var linkHashes []string
			if len(dagLeaf.Links) > 0 {
				linkHashes = make([]string, 0, len(dagLeaf.Links))
				linkHashes = append(linkHashes, dagLeaf.Links...)
				sort.Strings(linkHashes)
			}

			data := struct {
				Hash              string
				ItemName          string
				Type              LeafType
				ContentHash       []byte
				Content           []byte
				ClassicMerkleRoot []byte
				CurrentLinkCount  int
				LeafCount         int
				ContentSize       int64
				DagSize           int64
				Links             []string
				AdditionalData    map[string]string
			}{
				Hash:              dagLeaf.Hash,
				ItemName:          dagLeaf.ItemName,
				Type:              dagLeaf.Type,
				ContentHash:       dagLeaf.ContentHash,
				Content:           dagLeaf.Content,
				ClassicMerkleRoot: dagLeaf.ClassicMerkleRoot,
				CurrentLinkCount:  dagLeaf.CurrentLinkCount,
				LeafCount:         dagLeaf.LeafCount,
				ContentSize:       dagLeaf.ContentSize,
				DagSize:           dagLeaf.DagSize,
				Links:             linkHashes,
				AdditionalData:    SortMapByKeys(dagLeaf.AdditionalData),
			}

			serialized, err := cbor.Marshal(data)
			if err != nil {
				return fmt.Errorf("failed to serialize leaf %s for size verification: %w", dagLeaf.Hash, err)
			}
			childrenDagSize += int64(len(serialized))
		}

		tempLeafData := struct {
			ItemName         string
			Type             LeafType
			MerkleRoot       []byte
			CurrentLinkCount int
			LeafCount        int
			ContentSize      int64
			DagSize          int64
			ContentHash      []byte
			AdditionalData   []KeyValue
		}{
			ItemName:         leaf.ItemName,
			Type:             leaf.Type,
			MerkleRoot:       leaf.ClassicMerkleRoot,
			CurrentLinkCount: leaf.CurrentLinkCount,
			LeafCount:        leaf.LeafCount,
			ContentSize:      leaf.ContentSize,
			DagSize:          0,
			ContentHash:      leaf.ContentHash,
			AdditionalData:   SortMapForVerification(additionalData),
		}

		tempSerialized, err := cbor.Marshal(tempLeafData)
		if err != nil {
			return fmt.Errorf("failed to serialize root leaf for size verification: %w", err)
		}
		rootLeafSize := int64(len(tempSerialized))

		calculatedDagSize = childrenDagSize + rootLeafSize

		if leaf.ContentSize != calculatedContentSize {
			return fmt.Errorf("content size mismatch: stored %d, calculated %d", leaf.ContentSize, calculatedContentSize)
		}

		if leaf.DagSize != calculatedDagSize {
			return fmt.Errorf("dag size mismatch: stored %d, calculated %d", leaf.DagSize, calculatedDagSize)
		}
	}

	leafData := struct {
		ItemName         string
		Type             LeafType
		MerkleRoot       []byte
		CurrentLinkCount int
		LeafCount        int
		ContentSize      int64
		DagSize          int64
		ContentHash      []byte
		AdditionalData   []KeyValue
	}{
		ItemName:         leaf.ItemName,
		Type:             leaf.Type,
		MerkleRoot:       leaf.ClassicMerkleRoot,
		CurrentLinkCount: leaf.CurrentLinkCount,
		LeafCount:        leaf.LeafCount,
		ContentSize:      leaf.ContentSize,
		DagSize:          leaf.DagSize,
		ContentHash:      leaf.ContentHash,
		AdditionalData:   SortMapForVerification(additionalData),
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

	currentCid, err := cid.Decode(leaf.Hash)
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
			// Links are already sorted, iterate directly
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
		if link == hash {
			return true
		}
	}
	return false
}

func (leaf *DagLeaf) AddLink(hash string) {
	leaf.Links = append(leaf.Links, hash)
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
		LeafCount:         leaf.LeafCount,
		ContentSize:       leaf.ContentSize,
		DagSize:           leaf.DagSize,
		ParentHash:        leaf.ParentHash,
		Links:             make([]string, 0, len(leaf.Links)),
		AdditionalData:    make(map[string]string),
		Proofs:            make(map[string]*ClassicTreeBranch),
	}

	// Deep copy slices and maps
	cloned.Links = append(cloned.Links, leaf.Links...)
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

// EstimateSize returns approximate serialized size without full CBOR encoding
func (leaf *DagLeaf) EstimateSize() int {
	if leaf == nil {
		return 0
	}

	size := 0

	// Hash (CIDv1 string, typically ~64 bytes)
	size += len(leaf.Hash)

	// ItemName
	size += len(leaf.ItemName)

	// Type string (~10 bytes)
	size += len(string(leaf.Type))

	// Content (actual file/chunk data)
	size += len(leaf.Content)

	// ContentHash (32 bytes if present)
	if leaf.ContentHash != nil {
		size += len(leaf.ContentHash)
	}

	// ClassicMerkleRoot (32 bytes if present)
	if leaf.ClassicMerkleRoot != nil {
		size += len(leaf.ClassicMerkleRoot)
	}

	// Links (each is a CID string ~64 bytes)
	size += len(leaf.Links) * 64

	// AdditionalData (key-value pairs)
	for key, value := range leaf.AdditionalData {
		size += len(key) + len(value)
	}

	// Proofs (merkle proofs for children)
	// Each proof has a path with ~log2(n) siblings, each sibling is 32 bytes
	for _, proof := range leaf.Proofs {
		if proof != nil && proof.Proof != nil {
			// Estimate ~10 siblings per proof path (covers up to 1024 children)
			size += 10 * 32
		}
	}

	// Integer fields (CurrentLinkCount, LeafCount, ContentSize, DagSize)
	// These are variable-length encoded but estimate ~8 bytes each
	size += 32

	// Struct overhead and CBOR encoding overhead (~100 bytes)
	size += 100

	return size
}
