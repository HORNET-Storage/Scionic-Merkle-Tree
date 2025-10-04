package dag

import (
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	merkle_tree "github.com/HORNET-Storage/Scionic-Merkle-Tree/tree"
	cbor "github.com/fxamacker/cbor/v2"
)

type fileInfoDirEntry struct {
	fileInfo os.FileInfo
}

func (e fileInfoDirEntry) Name() string {
	return e.fileInfo.Name()
}

func (e fileInfoDirEntry) IsDir() bool {
	return e.fileInfo.IsDir()
}

func (e fileInfoDirEntry) Type() fs.FileMode {
	return e.fileInfo.Mode().Type()
}

func (e fileInfoDirEntry) Info() (fs.FileInfo, error) {
	return e.fileInfo, nil
}

func newDirEntry(path string) (fs.DirEntry, error) {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	return fileInfoDirEntry{fileInfo: fileInfo}, nil
}

func CreateDag(path string, timestampRoot bool) (*Dag, error) {
	var additionalData map[string]string = nil

	if timestampRoot {
		currentTime := time.Now().UTC()
		timeString := currentTime.Format(time.RFC3339)
		additionalData = map[string]string{
			"timestamp": timeString,
		}
	}

	dag, err := createDag(path, additionalData, nil)
	if err != nil {
		return nil, err
	}

	return dag, nil
}

func CreateDagAdvanced(path string, additionalData map[string]string) (*Dag, error) {
	dag, err := createDag(path, additionalData, nil)
	if err != nil {
		return nil, err
	}

	return dag, nil
}

// CreateDagCustom creates a DAG with custom metadata for each leaf
func CreateDagCustom(path string, rootAdditionalData map[string]string, processor LeafProcessor) (*Dag, error) {
	dag, err := createDag(path, rootAdditionalData, processor)
	if err != nil {
		return nil, err
	}

	return dag, nil
}

func createDag(path string, additionalData map[string]string, processor LeafProcessor) (*Dag, error) {
	dag := CreateDagBuilder()

	fileInfo, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	dirEntry, err := newDirEntry(path)
	if err != nil {
		return nil, err
	}

	parentPath := filepath.Dir(path)

	var leaf *DagLeaf

	if fileInfo.IsDir() {
		leaf, err = processDirectory(dirEntry, path, &parentPath, dag, true, additionalData, processor)
	} else {
		leaf, err = processFile(dirEntry, path, &parentPath, dag, true, additionalData, processor)
	}

	if err != nil {
		return nil, err
	}

	dag.AddLeaf(leaf, nil)
	rootHash := leaf.Hash

	return dag.BuildDag(rootHash), nil
}

func processEntry(entry fs.DirEntry, fullPath string, path *string, dag *DagBuilder, processor LeafProcessor) (*DagLeaf, error) {
	var result *DagLeaf
	var err error

	entryPath := filepath.Join(*path, entry.Name())

	if entry.IsDir() {
		result, err = processDirectory(entry, entryPath, path, dag, false, nil, processor)
	} else {
		result, err = processFile(entry, entryPath, path, dag, false, nil, processor)
	}

	if err != nil {
		return nil, err
	}

	return result, nil
}

func processDirectory(entry fs.DirEntry, fullPath string, path *string, dag *DagBuilder, isRoot bool, additionalData map[string]string, processor LeafProcessor) (*DagLeaf, error) {
	relPath, err := filepath.Rel(*path, fullPath)
	if err != nil {
		return nil, err
	}

	// Apply processor if provided and not root (root uses additionalData directly)
	if processor != nil && !isRoot {
		customData := processor(fullPath, relPath, entry, isRoot, DirectoryLeafType)
		if customData != nil {
			// Create additionalData if it's nil
			if additionalData == nil {
				additionalData = make(map[string]string)
			}

			// Merge custom data
			for k, v := range customData {
				additionalData[k] = v
			}
		}
	}

	builder := CreateDagLeafBuilder(relPath)
	builder.SetType(DirectoryLeafType)

	entries, err := os.ReadDir(fullPath)
	if err != nil {
		return nil, err
	}

	var result *DagLeaf

	for _, childEntry := range entries {
		leaf, err := processEntry(childEntry, filepath.Join(fullPath, childEntry.Name()), &fullPath, dag, processor)
		if err != nil {
			return nil, err
		}

		label := dag.GetNextAvailableLabel()
		builder.AddLink(label, leaf.Hash)
		leaf.SetLabel(label)
		dag.AddLeaf(leaf, nil)
	}

	if isRoot {
		result, err = builder.BuildRootLeaf(dag, additionalData)
	} else {
		result, err = builder.BuildLeaf(additionalData)
	}

	if err != nil {
		return nil, err
	}

	return result, nil
}

func processFile(entry fs.DirEntry, fullPath string, path *string, dag *DagBuilder, isRoot bool, additionalData map[string]string, processor LeafProcessor) (*DagLeaf, error) {
	relPath, err := filepath.Rel(*path, fullPath)
	if err != nil {
		return nil, err
	}

	// Apply processor if provided and not root (root uses additionalData directly)
	if processor != nil && !isRoot {
		customData := processor(fullPath, relPath, entry, isRoot, FileLeafType)
		if customData != nil {
			// Create additionalData if it's nil
			if additionalData == nil {
				additionalData = make(map[string]string)
			}

			// Merge custom data
			for k, v := range customData {
				additionalData[k] = v
			}
		}
	}

	var result *DagLeaf
	builder := CreateDagLeafBuilder(relPath)
	builder.SetType(FileLeafType)

	fileData, err := os.ReadFile(fullPath)
	if err != nil {
		return nil, err
	}

	builder.SetType(FileLeafType)

	if ChunkSize <= 0 {
		builder.SetData(fileData)
	} else {
		fileChunks := chunkFile(fileData, ChunkSize)

		if len(fileChunks) == 1 {
			builder.SetData(fileChunks[0])
		} else {
			for i, chunk := range fileChunks {
				chunkEntryPath := filepath.Join(relPath, strconv.Itoa(i))
				chunkBuilder := CreateDagLeafBuilder(chunkEntryPath)

				chunkBuilder.SetType(ChunkLeafType)
				chunkBuilder.SetData(chunk)

				// Chunks don't get custom metadata
				chunkLeaf, err := chunkBuilder.BuildLeaf(nil)
				if err != nil {
					return nil, err
				}

				label := dag.GetNextAvailableLabel()
				builder.AddLink(label, chunkLeaf.Hash)
				chunkLeaf.SetLabel(label)
				dag.AddLeaf(chunkLeaf, nil)
			}
		}
	}

	if isRoot {
		result, err = builder.BuildRootLeaf(dag, additionalData)
	} else {
		result, err = builder.BuildLeaf(additionalData)
	}

	if err != nil {
		return nil, err
	}

	return result, nil
}

func chunkFile(fileData []byte, chunkSize int) [][]byte {
	var chunks [][]byte
	fileSize := len(fileData)

	if chunkSize <= 0 {
		return [][]byte{fileData}
	}

	for i := 0; i < fileSize; i += chunkSize {
		end := i + chunkSize
		if end > fileSize {
			end = fileSize
		}
		chunks = append(chunks, fileData[i:end])
	}

	return chunks
}

func CreateDagBuilder() *DagBuilder {
	return &DagBuilder{
		Leafs: map[string]*DagLeaf{},
	}
}

func (b *DagBuilder) AddLeaf(leaf *DagLeaf, parentLeaf *DagLeaf) error {
	if parentLeaf != nil {
		label := GetLabel(leaf.Hash)
		_, exists := parentLeaf.Links[label]
		if !exists {
			parentLeaf.AddLink(leaf.Hash)
		}

		// If parent has more than one link, rebuild its merkle tree
		if len(parentLeaf.Links) > 1 {
			builder := merkle_tree.CreateTree()
			for _, link := range parentLeaf.Links {
				builder.AddLeaf(GetLabel(link), link)
			}

			merkleTree, leafMap, err := builder.Build()
			if err == nil {
				parentLeaf.MerkleTree = merkleTree
				parentLeaf.LeafMap = leafMap
				parentLeaf.ClassicMerkleRoot = merkleTree.Root
			}
		}
	}

	b.Leafs[leaf.Hash] = leaf
	return nil
}

func (b *DagBuilder) BuildDag(root string) *Dag {
	return &Dag{
		Leafs: b.Leafs,
		Root:  root,
	}
}

// verifyFullDag verifies a complete DAG by checking parent-child relationships
func (d *Dag) verifyFullDag() error {
	return d.IterateDag(func(leaf *DagLeaf, parent *DagLeaf) error {
		if leaf.Hash == d.Root {
			err := leaf.VerifyRootLeaf()
			if err != nil {
				return err
			}
		} else {
			err := leaf.VerifyLeaf()
			if err != nil {
				return err
			}

			if !parent.HasLink(leaf.Hash) {
				return fmt.Errorf("parent %s does not contain link to child %s", parent.Hash, leaf.Hash)
			}
		}

		return nil
	})
}

// verifyWithProofs verifies a partial DAG using stored Merkle proofs
func (d *Dag) verifyWithProofs() error {
	// First verify the root leaf
	rootLeaf := d.Leafs[d.Root]
	if err := rootLeaf.VerifyRootLeaf(); err != nil {
		return fmt.Errorf("root leaf failed to verify: %w", err)
	}

	// Verify each non-root leaf
	for _, leaf := range d.Leafs {
		if leaf.Hash == d.Root {
			continue
		}

		// First verify the leaf itself
		if err := leaf.VerifyLeaf(); err != nil {
			return fmt.Errorf("leaf %s failed to verify: %w", leaf.Hash, err)
		}

		// Then verify the path to root
		current := leaf
		for current.Hash != d.Root {
			// Find parent in this partial DAG
			var parent *DagLeaf
			for _, potential := range d.Leafs {
				if potential.HasLink(current.Hash) {
					parent = potential
					break
				}
			}
			if parent == nil {
				return fmt.Errorf("broken path to root for leaf %s", leaf.Hash)
			}

			// Verify parent leaf
			if parent.Hash != d.Root {
				if err := parent.VerifyLeaf(); err != nil {
					return fmt.Errorf("parent leaf %s failed to verify: %w", parent.Hash, err)
				}
			}

			// Only verify merkle proof if parent has multiple children
			// according to its CurrentLinkCount (which is part of its hash)
			if parent.CurrentLinkCount > 1 {
				proof, hasProof := parent.Proofs[current.Hash]

				if !hasProof {
					return fmt.Errorf("missing merkle proof for node %s in partial DAG", current.Hash)
				}

				err := parent.VerifyBranch(proof)
				if err != nil {
					return fmt.Errorf("invalid merkle proof for node %s: %w", current.Hash, err)
				}
			}

			current = parent
		}
	}

	return nil
}

// Verify checks the integrity of the DAG, automatically choosing between full and partial verification
func (d *Dag) Verify() error {
	if d.IsPartial() {
		// Use more thorough verification with proofs for partial DAGs
		return d.verifyWithProofs()
	}
	// Use simpler verification for full DAGs
	return d.verifyFullDag()
}

func (dag *Dag) CreateDirectory(path string) error {
	rootHash := dag.Root
	rootLeaf := dag.Leafs[rootHash]

	err := rootLeaf.CreateDirectoryLeaf(path, dag)
	if err != nil {
		return err
	}

	return nil
}

func ReadDag(path string) (*Dag, error) {
	fileData, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("could not read file: %w", err)
	}

	var result Dag
	if err := cbor.Unmarshal(fileData, &result); err != nil {
		return nil, fmt.Errorf("could not decode Dag: %w", err)
	}

	return &result, nil
}

func (dag *Dag) GetContentFromLeaf(leaf *DagLeaf) ([]byte, error) {
	var content []byte

	if len(leaf.Links) > 0 {
		// For chunked files, sort links by label and concatenate content from all chunks
		var sortedLinks []struct {
			Label int
			Link  string
		}

		for label, link := range leaf.Links {
			labelNum, err := strconv.Atoi(label)
			if err != nil {
				return nil, fmt.Errorf("invalid link label: %s", label)
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
				return nil, fmt.Errorf("invalid link: %s", item.Link)
			}

			content = append(content, childLeaf.Content...)
		}
	} else if len(leaf.Content) > 0 {
		// For single-chunk files, return content directly
		content = leaf.Content
	}

	return content, nil
}

func (d *Dag) IterateDag(processLeaf func(leaf *DagLeaf, parent *DagLeaf) error) error {
	var iterate func(leafHash string, parentHash *string) error
	iterate = func(leafHash string, parentHash *string) error {
		leaf, exists := d.Leafs[leafHash]
		if !exists {
			return fmt.Errorf("child is missing when iterating dag")
		}

		var parent *DagLeaf
		if parentHash != nil {
			parent = d.Leafs[*parentHash]
		}

		err := processLeaf(leaf, parent)
		if err != nil {
			return err
		}

		childHashes := []string{}
		for _, childHash := range leaf.Links {
			childHashes = append(childHashes, childHash)
		}

		sort.Slice(childHashes, func(i, j int) bool {
			numI, _ := strconv.Atoi(strings.Split(childHashes[i], ":")[0])
			numJ, _ := strconv.Atoi(strings.Split(childHashes[j], ":")[0])
			return numI < numJ
		})

		for _, childHash := range childHashes {
			err := iterate(childHash, &leaf.Hash)
			if err != nil {
				return err
			}
		}

		return nil
	}

	return iterate(d.Root, nil)
}

// IsPartial returns true if this DAG is a partial DAG (has fewer leaves than the total count)
func (d *Dag) IsPartial() bool {
	// Get the root leaf
	rootLeaf := d.Leafs[d.Root]
	if rootLeaf == nil {
		return true // If root leaf is missing, it's definitely partial
	}

	// Check if the number of leaves in the DAG matches the total leaf count
	return len(d.Leafs) < rootLeaf.LeafCount
}

// pruneIrrelevantLinks removes links that aren't needed for partial verification
func (d *Dag) pruneIrrelevantLinks(relevantHashes map[string]bool) {
	for _, leaf := range d.Leafs {
		// Create new map for relevant links
		prunedLinks := make(map[string]string)

		// Only keep links that are relevant
		for label, hash := range leaf.Links {
			if relevantHashes[GetHash(hash)] {
				prunedLinks[label] = hash
			}
		}

		// Only modify the Links map, keep everything else as is
		// since they're part of the leaf's identity
		leaf.Links = prunedLinks
	}
}

// buildVerificationBranch creates a branch containing the leaf and its verification path
func (d *Dag) buildVerificationBranch(leaf *DagLeaf) (*DagBranch, error) {
	// Clone the root leaf first to ensure it has all fields
	rootLeaf := d.Leafs[d.Root].Clone()

	branch := &DagBranch{
		Leaf: leaf.Clone(),
		Path: make([]*DagLeaf, 0),
	}

	// Always add root leaf to path
	branch.Path = append(branch.Path, rootLeaf)

	// Find path to root through parent nodes
	current := leaf
	for current.Hash != d.Root {
		// Find parent in this partial DAG, not the original
		var parent *DagLeaf
		for _, potential := range d.Leafs {
			if potential.HasLink(current.Hash) {
				parent = potential
				break
			}
		}
		if parent == nil {
			return nil, fmt.Errorf("failed to find parent for leaf %s", current.Hash)
		}

		// Clone parent before any modifications
		parentClone := parent.Clone()

		// If parent has multiple children according to CurrentLinkCount,
		// we must generate and store a proof since this is our only chance
		if parent.CurrentLinkCount > 1 {
			// Find the label for current in parent's links
			var label string
			for l, h := range parent.Links {
				if h == current.Hash {
					label = l
					break
				}
			}
			if label == "" {
				return nil, fmt.Errorf("unable to find label for key")
			}

			// Build merkle tree with all current links
			builder := merkle_tree.CreateTree()
			for l, h := range parent.Links {
				builder.AddLeaf(l, h)
			}
			merkleTree, _, err := builder.Build()
			if err != nil {
				return nil, err
			}

			// Get proof for the current leaf
			index, exists := merkleTree.GetIndexForKey(label)
			if !exists {
				return nil, fmt.Errorf("unable to find index for key %s", label)
			}

			// Store proof in parent clone
			if parentClone.Proofs == nil {
				parentClone.Proofs = make(map[string]*ClassicTreeBranch)
			}

			proof := &ClassicTreeBranch{
				Leaf:  current.Hash,
				Proof: merkleTree.Proofs[index],
			}

			// Store proof using the hash
			parentClone.Proofs[current.Hash] = proof
		}

		// Always add parent to path so its proofs get merged
		branch.Path = append(branch.Path, parentClone)

		current = parent
	}

	return branch, nil
}

// addBranchToPartial adds a branch to the partial DAG
func (d *Dag) addBranchToPartial(branch *DagBranch, partial *Dag) error {
	// Add leaf if not present
	if _, exists := partial.Leafs[branch.Leaf.Hash]; !exists {
		partial.Leafs[branch.Leaf.Hash] = branch.Leaf
	}

	// Add all path nodes (including root) and merge their proofs
	for i := 0; i < len(branch.Path); i++ {
		pathNode := branch.Path[i]
		if existingNode, exists := partial.Leafs[pathNode.Hash]; exists {
			// Create a new node with merged proofs
			mergedNode := existingNode.Clone()
			if mergedNode.Proofs == nil {
				mergedNode.Proofs = make(map[string]*ClassicTreeBranch)
			}
			if pathNode.Proofs != nil {
				for k, v := range pathNode.Proofs {
					mergedNode.Proofs[k] = v
				}
			}

			// Update the node in the map
			partial.Leafs[pathNode.Hash] = mergedNode

		} else {
			partial.Leafs[pathNode.Hash] = pathNode

		}
	}

	return nil
}

// GetPartial returns a new DAG containing only the requested leaves and their verification paths
func (d *Dag) GetPartial(start, end int) (*Dag, error) {
	if start == end {
		return nil, fmt.Errorf("invalid range: indices cannot be the same")
	}

	if start < 0 || end < 0 {
		return nil, fmt.Errorf("invalid range: indices cannot be negative")
	}

	if start > end {
		return nil, fmt.Errorf("invalid range: start cannot be greater than end")
	}

	rootLeaf := d.Leafs[d.Root]
	if start >= rootLeaf.LeafCount || end > rootLeaf.LeafCount {
		return nil, fmt.Errorf("invalid range: indices cannot be greater than the overall leaf count")
	}

	partialDag := &Dag{
		Leafs: make(map[string]*DagLeaf),
		Root:  d.Root,
	}

	// Track hashes that are relevant for verification
	relevantHashes := make(map[string]bool)
	relevantHashes[GetHash(d.Root)] = true

	// Process each requested leaf
	for i := start; i <= end; i++ {
		// Find and validate leaf
		var targetLeaf *DagLeaf
		if i == 0 {
			targetLeaf = d.Leafs[d.Root]
		} else {
			label := strconv.Itoa(i)
			for _, leaf := range d.Leafs {
				if GetLabel(leaf.Hash) == label {
					targetLeaf = leaf
					break
				}
			}
		}

		if targetLeaf == nil {
			continue
		}

		// Add target leaf hash to relevant hashes
		relevantHashes[GetHash(targetLeaf.Hash)] = true

		// Build verification path
		branch, err := d.buildVerificationBranch(targetLeaf)
		if err != nil {
			return nil, err
		}

		// Track hashes from verification path
		for _, pathNode := range branch.Path {
			relevantHashes[GetHash(pathNode.Hash)] = true
		}

		// Track hashes from Merkle proofs
		for _, proof := range branch.MerkleProofs {
			// Add the leaf hash
			relevantHashes[GetHash(proof.Leaf)] = true
			// Add all sibling hashes from the proof
			for _, sibling := range proof.Proof.Siblings {
				relevantHashes[string(sibling)] = true
			}
		}

		// Add branch to partial DAG
		err = d.addBranchToPartial(branch, partialDag)
		if err != nil {
			return nil, err
		}
	}

	// Prune irrelevant links from the partial DAG
	partialDag.pruneIrrelevantLinks(relevantHashes)

	return partialDag, nil
}

// getPartialLeafSequence returns an ordered sequence of leaves for transmission from a partial DAG
// This is an internal method used by GetLeafSequence when dealing with partial DAGs
func (d *Dag) getPartialLeafSequence() []*TransmissionPacket {
	var sequence []*TransmissionPacket

	// Get the root leaf
	rootLeaf := d.Leafs[d.Root]
	if rootLeaf == nil {
		return sequence // Return empty sequence if root leaf is missing
	}

	// First, build a map of proofs organized by parent hash and child hash
	// This will allow us to look up the proof for a specific child when creating its packet
	proofMap := make(map[string]map[string]*ClassicTreeBranch)

	// Populate the proof map from all leaves in the partial DAG
	for _, leaf := range d.Leafs {
		if len(leaf.Proofs) > 0 {
			// Create an entry for this parent if it doesn't exist
			if _, exists := proofMap[leaf.Hash]; !exists {
				proofMap[leaf.Hash] = make(map[string]*ClassicTreeBranch)
			}

			// Add all proofs from this leaf to the map
			for childHash, proof := range leaf.Proofs {
				proofMap[leaf.Hash][childHash] = proof
			}
		}
	}

	// Now perform BFS traversal similar to the full DAG method
	visited := make(map[string]bool)

	// Start with the root
	rootLeafClone := rootLeaf.Clone()

	// We need to preserve the original links for the root leaf
	// because they're part of its identity and hash calculation
	originalLinks := make(map[string]string)
	for k, v := range rootLeaf.Links {
		originalLinks[k] = v
	}

	// Clear links for transmission (they'll be reconstructed on the receiving end)
	rootLeafClone.Links = make(map[string]string)

	// We need to preserve these fields for verification
	// but clear proofs for the root packet - they'll be sent with child packets
	originalMerkleRoot := rootLeafClone.ClassicMerkleRoot
	originalLatestLabel := rootLeafClone.LatestLabel
	originalLeafCount := rootLeafClone.LeafCount

	rootLeafClone.Proofs = nil

	// Restore the critical fields
	rootLeafClone.ClassicMerkleRoot = originalMerkleRoot
	rootLeafClone.LatestLabel = originalLatestLabel
	rootLeafClone.LeafCount = originalLeafCount

	rootPacket := &TransmissionPacket{
		Leaf:       rootLeafClone,
		ParentHash: "", // Root has no parent
		Proofs:     make(map[string]*ClassicTreeBranch),
	}
	sequence = append(sequence, rootPacket)
	visited[d.Root] = true

	// Restore the original links for the root leaf in the DAG
	rootLeaf.Links = originalLinks

	// BFS traversal
	queue := []string{d.Root}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		currentLeaf := d.Leafs[current]

		// Sort links for deterministic order
		var sortedLinks []string
		for _, link := range currentLeaf.Links {
			sortedLinks = append(sortedLinks, link)
		}
		sort.Strings(sortedLinks)

		// Process each child
		for _, childHash := range sortedLinks {
			if !visited[childHash] {
				childLeaf := d.Leafs[childHash]
				if childLeaf == nil {
					continue // Skip if child leaf doesn't exist in this partial DAG
				}

				// Clone the leaf and clear its links for transmission
				leafClone := childLeaf.Clone()
				leafClone.Links = make(map[string]string)
				leafClone.Proofs = nil // Clear proofs from the leaf

				packet := &TransmissionPacket{
					Leaf:       leafClone,
					ParentHash: current,
					Proofs:     make(map[string]*ClassicTreeBranch),
				}

				// Add the proof for this specific child from the proof map
				if parentProofs, exists := proofMap[current]; exists {
					if proof, hasProof := parentProofs[childHash]; hasProof {
						packet.Proofs[childHash] = proof
					}
				}

				sequence = append(sequence, packet)
				visited[childHash] = true
				queue = append(queue, childHash)
			}
		}
	}

	return sequence
}

// GetLeafSequence returns an ordered sequence of leaves for transmission
// Each packet contains a leaf, its parent hash, and any proofs needed for verification
func (d *Dag) GetLeafSequence() []*TransmissionPacket {
	// Check if this is a partial DAG
	if d.IsPartial() {
		// Use specialized method for partial DAGs
		return d.getPartialLeafSequence()
	}

	// Original implementation for complete DAGs
	var sequence []*TransmissionPacket
	visited := make(map[string]bool)

	rootLeaf := d.Leafs[d.Root]
	if rootLeaf == nil {
		return sequence
	}

	totalLeafCount := rootLeaf.LeafCount

	rootLeafClone := rootLeaf.Clone()
	rootLeafClone.Links = make(map[string]string)

	rootPacket := &TransmissionPacket{
		Leaf:       rootLeafClone,
		ParentHash: "",
		Proofs:     make(map[string]*ClassicTreeBranch),
	}
	sequence = append(sequence, rootPacket)
	visited[d.Root] = true

	queue := []string{d.Root}
	for len(queue) > 0 && len(sequence) <= totalLeafCount {
		current := queue[0]
		queue = queue[1:]

		currentLeaf := d.Leafs[current]

		var sortedLinks []string
		for _, link := range currentLeaf.Links {
			sortedLinks = append(sortedLinks, link)
		}
		sort.Strings(sortedLinks)

		for _, childHash := range sortedLinks {
			if !visited[childHash] && len(sequence) <= totalLeafCount {
				branch, err := d.buildVerificationBranch(d.Leafs[childHash])
				if err != nil {
					continue
				}

				leafClone := d.Leafs[childHash].Clone()
				leafClone.Links = make(map[string]string)

				packet := &TransmissionPacket{
					Leaf:       leafClone,
					ParentHash: current,
					Proofs:     make(map[string]*ClassicTreeBranch),
				}

				for _, pathNode := range branch.Path {
					if pathNode.Proofs != nil {
						for k, v := range pathNode.Proofs {
							packet.Proofs[k] = v
						}
					}
				}

				sequence = append(sequence, packet)
				visited[childHash] = true
				queue = append(queue, childHash)
			}
		}
	}

	return sequence
}

// VerifyTransmissionPacket verifies a transmission packet independently
func (d *Dag) VerifyTransmissionPacket(packet *TransmissionPacket) error {
	if packet.ParentHash == "" {
		if err := packet.Leaf.VerifyRootLeaf(); err != nil {
			return fmt.Errorf("transmission packet root leaf verification failed: %w", err)
		}
	} else {
		if err := packet.Leaf.VerifyLeaf(); err != nil {
			return fmt.Errorf("transmission packet leaf verification failed: %w", err)
		}

		if parent, exists := d.Leafs[packet.ParentHash]; exists && len(parent.Links) > 1 {
			proof, hasProof := packet.Proofs[packet.Leaf.Hash]
			if !hasProof {
				return fmt.Errorf("missing merkle proof for leaf %s in transmission packet", packet.Leaf.Hash)
			}

			if err := parent.VerifyBranch(proof); err != nil {
				return fmt.Errorf("invalid merkle proof for leaf %s: %w", packet.Leaf.Hash, err)
			}
		}
	}

	return nil
}

// ApplyTransmissionPacket applies a transmission packet to the DAG
func (d *Dag) ApplyTransmissionPacket(packet *TransmissionPacket) {
	d.Leafs[packet.Leaf.Hash] = packet.Leaf

	if packet.ParentHash != "" {
		if parent, exists := d.Leafs[packet.ParentHash]; exists {
			label := GetLabel(packet.Leaf.Hash)
			if label != "" {
				parent.Links[label] = packet.Leaf.Hash
			}
		}
	}

	for leafHash, proof := range packet.Proofs {
		for _, leaf := range d.Leafs {
			if leaf.HasLink(leafHash) {
				if leaf.Proofs == nil {
					leaf.Proofs = make(map[string]*ClassicTreeBranch)
				}
				leaf.Proofs[leafHash] = proof
				break
			}
		}
	}
}

// ApplyAndVerifyTransmissionPacket verifies then applies a transmission packet
func (d *Dag) ApplyAndVerifyTransmissionPacket(packet *TransmissionPacket) error {
	if err := d.VerifyTransmissionPacket(packet); err != nil {
		return err
	}

	d.ApplyTransmissionPacket(packet)
	return nil
}

func (d *Dag) RemoveAllContent() {
	for _, leaf := range d.Leafs {
		leaf.Content = nil
	}
}

// GetBatchedLeafSequence returns an ordered sequence of batched transmission packets
// Each batch contains multiple related leaves that share common branches for efficient transmission
func (d *Dag) GetBatchedLeafSequence() []*BatchedTransmissionPacket {
	if BatchSize <= 0 {
		individualPackets := d.GetLeafSequence()
		var batchedPackets []*BatchedTransmissionPacket
		for _, packet := range individualPackets {
			batchedPackets = append(batchedPackets, &BatchedTransmissionPacket{
				Leaves:        []*DagLeaf{packet.Leaf},
				Relationships: map[string]string{packet.Leaf.Hash: packet.ParentHash},
				Proofs:        packet.Proofs,
			})
		}
		return batchedPackets
	}

	var sequence []*BatchedTransmissionPacket
	visited := make(map[string]bool)

	rootLeaf := d.Leafs[d.Root]
	if rootLeaf == nil {
		return sequence
	}

	rootLeafClone := rootLeaf.Clone()
	rootLeafClone.Links = make(map[string]string)

	rootBatch := &BatchedTransmissionPacket{
		Leaves:        []*DagLeaf{rootLeafClone},
		Relationships: map[string]string{rootLeafClone.Hash: ""},
		Proofs:        make(map[string]*ClassicTreeBranch),
	}
	sequence = append(sequence, rootBatch)
	visited[d.Root] = true

	queue := []string{d.Root}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		currentLeaf := d.Leafs[current]

		var children []string
		for _, link := range currentLeaf.Links {
			if !visited[link] {
				children = append(children, link)
			}
		}
		sort.Strings(children)

		d.batchChildren(children, current, &sequence, visited, &queue)
	}

	return sequence
}

// batchChildren groups children into batches based on size and shared branches
func (d *Dag) batchChildren(children []string, parentHash string, sequence *[]*BatchedTransmissionPacket, visited map[string]bool, queue *[]string) {
	if len(children) == 0 {
		return
	}

	d.batchChildrenWithParentIncluded(children, parentHash, sequence, visited, queue)
}

// batchChildrenWithParentIncluded splits children across batches but includes parent in each batch
// This allows incremental transmission while maintaining Merkle proof validity
func (d *Dag) batchChildrenWithParentIncluded(children []string, parentHash string, sequence *[]*BatchedTransmissionPacket, visited map[string]bool, queue *[]string) {
	parentLeaf := d.Leafs[parentHash]
	if parentLeaf == nil {
		return
	}

	currentBatch := &BatchedTransmissionPacket{
		Leaves:        make([]*DagLeaf, 0),
		Relationships: make(map[string]string),
		Proofs:        make(map[string]*ClassicTreeBranch),
	}

	parentClone := parentLeaf.Clone()
	parentClone.Links = make(map[string]string)
	currentBatch.Leaves = append(currentBatch.Leaves, parentClone)

	for _, childHash := range children {
		childLeaf := d.Leafs[childHash]
		if childLeaf == nil {
			continue
		}

		leafClone := childLeaf.Clone()
		leafClone.Links = make(map[string]string)

		// Check if adding this leaf would exceed batch size limit
		tempBatch := &BatchedTransmissionPacket{
			Leaves:        append(currentBatch.Leaves, leafClone),
			Relationships: make(map[string]string),
			Proofs:        make(map[string]*ClassicTreeBranch),
		}

		// Copy existing relationships
		for k, v := range currentBatch.Relationships {
			tempBatch.Relationships[k] = v
		}
		tempBatch.Relationships[childHash] = parentHash

		// Copy existing proofs
		for k, v := range currentBatch.Proofs {
			tempBatch.Proofs[k] = v
		}

		exceedsLimit := false
		if BatchSize > 0 {
			estimatedSize := d.estimateBatchSize(tempBatch)
			safetyMargin := BatchSize / 5
			effectiveLimit := BatchSize - safetyMargin
			exceedsLimit = estimatedSize > effectiveLimit
		}

		if exceedsLimit {
			// Current batch is full, add it to sequence and start a new batch
			if len(currentBatch.Leaves) > 1 { // Only add if it has children beyond the parent
				*sequence = append(*sequence, currentBatch)
			}

			// Start a new batch with just the parent
			currentBatch = &BatchedTransmissionPacket{
				Leaves:        []*DagLeaf{parentClone},
				Relationships: make(map[string]string),
				Proofs:        make(map[string]*ClassicTreeBranch),
			}
		}

		// Add the child to current batch
		currentBatch.Leaves = append(currentBatch.Leaves, leafClone)
		currentBatch.Relationships[childHash] = parentHash

		childCountInBatch := 0
		for _, leaf := range currentBatch.Leaves[1:] {
			if _, exists := currentBatch.Relationships[leaf.Hash]; exists {
				childCountInBatch++
			}
		}

		if childCountInBatch > 1 {
			for leafHash := range currentBatch.Proofs {
				if _, exists := currentBatch.Relationships[leafHash]; exists {
					delete(currentBatch.Proofs, leafHash)
				}
			}

			builder := merkle_tree.CreateTree()
			childLabels := make([]string, 0)
			childHashes := make([]string, 0)

			type childInfo struct {
				label string
				hash  string
			}
			var children []childInfo

			for _, leaf := range currentBatch.Leaves[1:] {
				if _, exists := currentBatch.Relationships[leaf.Hash]; exists {
					label := GetLabel(leaf.Hash)
					if label != "" {
						children = append(children, childInfo{label: label, hash: leaf.Hash})
					}
				}
			}

			sort.Slice(children, func(i, j int) bool {
				return children[i].label < children[j].label
			})

			for _, child := range children {
				builder.AddLeaf(child.label, child.hash)
				childLabels = append(childLabels, child.label)
				childHashes = append(childHashes, child.hash)
			}

			if len(childLabels) > 1 {
				merkleTree, _, err := builder.Build()
				if err == nil {
					for i, childHash := range childHashes {
						currentBatch.Proofs[childHash] = &ClassicTreeBranch{
							Leaf:  childHash,
							Proof: merkleTree.Proofs[i],
						}
					}
				}
			}
		}

		visited[childHash] = true
		*queue = append(*queue, childHash)
	}

	if len(currentBatch.Leaves) > 0 {
		*sequence = append(*sequence, currentBatch)
	}
}

// estimateBatchSize provides a rough estimate of a batched transmission packet's serialized size in bytes
func (d *Dag) estimateBatchSize(batch *BatchedTransmissionPacket) int {
	size := 0

	// Estimate leaves (more accurate estimation)
	for _, leaf := range batch.Leaves {
		// Base size for leaf structure
		size += 200 // CBOR overhead for leaf structure

		// Content fields
		size += len(leaf.Hash)
		size += len(leaf.ItemName)
		size += len(leaf.Content)
		size += len(leaf.ClassicMerkleRoot)

		// Additional data (key-value pairs)
		size += len(leaf.AdditionalData) * 100 // more generous estimate

		// Fixed fields
		size += 50 // for Type, CurrentLinkCount, etc.
	}

	// Estimate relationships (key + value strings with CBOR overhead)
	for k, v := range batch.Relationships {
		size += len(k) + len(v) + 20 // CBOR overhead
	}

	// Estimate proofs (each proof has significant overhead)
	for _, proof := range batch.Proofs {
		size += len(proof.Leaf) + 100 // proof structure overhead
		if proof.Proof != nil {
			size += len(proof.Proof.Siblings) * 35 // hash size + CBOR overhead
			size += 50                             // proof metadata
		}
	}

	// Add significant overhead for CBOR serialization structure
	size += 200 // CBOR map/array overhead

	return size
}

// VerifyBatchedTransmissionPacket verifies a batched transmission packet independently
func (d *Dag) VerifyBatchedTransmissionPacket(packet *BatchedTransmissionPacket) error {
	parentStates := make(map[string]*DagLeaf)

	// Build temporary parent leaves with their links reconstructed from relationships
	for childHash, parentHash := range packet.Relationships {
		if parentHash == "" {
			continue
		}

		if _, exists := parentStates[parentHash]; !exists {
			// Find parent in batch or existing DAG
			var parentLeaf *DagLeaf
			if batchParent := packet.findLeafByHash(parentHash); batchParent != nil {
				parentLeaf = batchParent.Clone()
			} else if dagParent, exists := d.Leafs[parentHash]; exists {
				parentLeaf = dagParent.Clone()
			} else {
				return fmt.Errorf("parent %s not found for leaf %s in batched transmission packet", parentHash, childHash)
			}

			// Reconstruct links for this parent based on relationships in the batch
			parentLeaf.Links = make(map[string]string)
			for cHash, pHash := range packet.Relationships {
				if pHash == parentHash {
					if childLeaf := packet.findLeafByHash(cHash); childLeaf != nil {
						label := GetLabel(childLeaf.Hash)
						if label != "" {
							parentLeaf.Links[label] = childLeaf.Hash
						}
					}
				}
			}

			// Rebuild Merkle tree if parent has multiple children
			if len(parentLeaf.Links) > 1 {
				builder := merkle_tree.CreateTree()
				for label, hash := range parentLeaf.Links {
					builder.AddLeaf(label, hash)
				}
				merkleTree, leafMap, err := builder.Build()
				if err != nil {
					return fmt.Errorf("failed to rebuild merkle tree for parent %s: %w", parentHash, err)
				}
				parentLeaf.MerkleTree = merkleTree
				parentLeaf.LeafMap = leafMap
				parentLeaf.ClassicMerkleRoot = merkleTree.Root
			}

			parentStates[parentHash] = parentLeaf
		}
	}

	// Now verify each leaf
	for childHash, parentHash := range packet.Relationships {
		childLeaf := packet.findLeafByHash(childHash)
		if childLeaf == nil {
			continue
		}

		if parentHash == "" {
			if err := childLeaf.VerifyRootLeaf(); err != nil {
				return fmt.Errorf("batched transmission packet root leaf %s verification failed: %w", childHash, err)
			}
		} else {
			if err := childLeaf.VerifyLeaf(); err != nil {
				return fmt.Errorf("batched transmission packet leaf %s verification failed: %w", childHash, err)
			}

			// Get the parent state for verification
			parentLeaf, exists := parentStates[parentHash]
			if !exists {
				return fmt.Errorf("parent state not available for leaf %s", childHash)
			}

			// If parent has multiple children in this batch, verify the merkle proof
			if len(parentLeaf.Links) > 1 {
				proof, hasProof := packet.Proofs[childHash]
				if !hasProof {
					return fmt.Errorf("missing merkle proof for leaf %s in batched transmission packet", childHash)
				}

				if err := parentLeaf.VerifyBranch(proof); err != nil {
					return fmt.Errorf("invalid merkle proof for leaf %s: %w", childHash, err)
				}
			}
		}
	}

	return nil
}

// ApplyBatchedTransmissionPacket applies a batched transmission packet to the DAG
func (d *Dag) ApplyBatchedTransmissionPacket(packet *BatchedTransmissionPacket) {
	parentsToUpdate := make(map[string]bool)

	for _, leaf := range packet.Leaves {
		// Check if leaf already exists
		if existingLeaf, exists := d.Leafs[leaf.Hash]; exists {
			// Merge links from the new leaf into the existing one
			if existingLeaf.Links == nil {
				existingLeaf.Links = make(map[string]string)
			}
			for k, v := range leaf.Links {
				existingLeaf.Links[k] = v
			}
			// Merge proofs from the new leaf into the existing one
			if existingLeaf.Proofs == nil {
				existingLeaf.Proofs = make(map[string]*ClassicTreeBranch)
			}
			for k, v := range leaf.Proofs {
				existingLeaf.Proofs[k] = v
			}
			// Update other fields if needed (Merkle tree, etc.)
			if leaf.MerkleTree != nil {
				existingLeaf.MerkleTree = leaf.MerkleTree
				existingLeaf.LeafMap = leaf.LeafMap
				existingLeaf.ClassicMerkleRoot = leaf.ClassicMerkleRoot
			}
			// Don't overwrite the existing leaf, just update it
		} else {
			// Leaf doesn't exist, add it
			d.Leafs[leaf.Hash] = leaf
		}

		// Update parent links
		if parentHash, exists := packet.Relationships[leaf.Hash]; exists && parentHash != "" {
			if parent, parentExists := d.Leafs[parentHash]; parentExists {
				label := GetLabel(leaf.Hash)
				if label != "" {
					parent.Links[label] = leaf.Hash
					parentsToUpdate[parentHash] = true
				}
			}
		}
	}

	// Apply proofs to parent leaves
	for childHash, proof := range packet.Proofs {
		if parentHash, exists := packet.Relationships[childHash]; exists {
			if parent := d.Leafs[parentHash]; parent != nil {
				if parent.Proofs == nil {
					parent.Proofs = make(map[string]*ClassicTreeBranch)
				}
				parent.Proofs[childHash] = proof
			}
		}
	}
}

// ApplyAndVerifyBatchedTransmissionPacket verifies then applies a batched transmission packet
func (d *Dag) ApplyAndVerifyBatchedTransmissionPacket(packet *BatchedTransmissionPacket) error {
	if err := d.VerifyBatchedTransmissionPacket(packet); err != nil {
		return err
	}

	d.ApplyBatchedTransmissionPacket(packet)
	return nil
}

// findLeafByHash finds a leaf in the packet by its hash
func (packet *BatchedTransmissionPacket) findLeafByHash(hash string) *DagLeaf {
	for _, leaf := range packet.Leaves {
		if leaf.Hash == hash {
			return leaf
		}
	}
	return nil
}

// GetRootLeaf returns the root leaf from the batch.
func (packet *BatchedTransmissionPacket) GetRootLeaf() *DagLeaf {
	if packet == nil || len(packet.Leaves) == 0 {
		return nil
	}

	parentHashes := make(map[string]bool)
	for _, parentHash := range packet.Relationships {
		parentHashes[parentHash] = true
	}

	for _, leaf := range packet.Leaves {
		if !parentHashes[leaf.Hash] {
			return leaf
		}
	}

	for _, leaf := range packet.Leaves {
		if leaf.Type == DirectoryLeafType {
			return leaf
		}
	}

	return packet.Leaves[0]
}
