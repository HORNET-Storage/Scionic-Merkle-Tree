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

	dag, err := createDag(path, additionalData)
	if err != nil {
		return nil, err
	}

	return dag, nil
}

func CreateDagAdvanced(path string, additionalData map[string]string) (*Dag, error) {
	dag, err := createDag(path, additionalData)
	if err != nil {
		return nil, err
	}

	return dag, nil
}

func createDag(path string, additionalData map[string]string) (*Dag, error) {
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
		leaf, err = processDirectory(dirEntry, &parentPath, dag, true, additionalData)
	} else {
		leaf, err = processFile(dirEntry, &parentPath, dag, true, additionalData)
	}

	if err != nil {
		return nil, err
	}

	dag.AddLeaf(leaf, nil)
	rootHash := leaf.Hash

	return dag.BuildDag(rootHash), nil
}

func processEntry(entry fs.DirEntry, path *string, dag *DagBuilder) (*DagLeaf, error) {
	var result *DagLeaf
	var err error

	if entry.IsDir() {
		result, err = processDirectory(entry, path, dag, false, nil)
	} else {
		result, err = processFile(entry, path, dag, false, nil)
	}

	if err != nil {
		return nil, err
	}

	return result, nil
}

func processDirectory(entry fs.DirEntry, path *string, dag *DagBuilder, isRoot bool, additionalData map[string]string) (*DagLeaf, error) {
	entryPath := filepath.Join(*path, entry.Name())

	relPath, err := filepath.Rel(*path, entryPath)
	if err != nil {
		return nil, err
	}

	builder := CreateDagLeafBuilder(relPath)
	builder.SetType(DirectoryLeafType)

	entries, err := os.ReadDir(entryPath)
	if err != nil {
		return nil, err
	}

	var result *DagLeaf

	for _, entry := range entries {
		leaf, err := processEntry(entry, &entryPath, dag)
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
		result, err = builder.BuildLeaf(nil)
	}

	if err != nil {
		return nil, err
	}

	return result, nil
}

func processFile(entry fs.DirEntry, path *string, dag *DagBuilder, isRoot bool, additionalData map[string]string) (*DagLeaf, error) {
	entryPath := filepath.Join(*path, entry.Name())

	relPath, err := filepath.Rel(*path, entryPath)
	if err != nil {
		return nil, err
	}

	var result *DagLeaf
	builder := CreateDagLeafBuilder(relPath)
	builder.SetType(FileLeafType)

	fileData, err := os.ReadFile(entryPath)
	if err != nil {
		return nil, err
	}

	builder.SetType(FileLeafType)
	fileChunks := chunkFile(fileData, ChunkSize)

	if len(fileChunks) == 1 {
		builder.SetData(fileChunks[0])
	} else {
		for i, chunk := range fileChunks {
			chunkEntryPath := filepath.Join(relPath, strconv.Itoa(i))
			chunkBuilder := CreateDagLeafBuilder(chunkEntryPath)

			chunkBuilder.SetType(ChunkLeafType)
			chunkBuilder.SetData(chunk)

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

	if isRoot {
		result, err = builder.BuildRootLeaf(dag, additionalData)
	} else {
		result, err = builder.BuildLeaf(nil)
	}

	if err != nil {
		return nil, err
	}

	return result, nil
}

func chunkFile(fileData []byte, chunkSize int) [][]byte {
	var chunks [][]byte
	fileSize := len(fileData)

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
				// Try both the full hash and the hash without label
				var proof *ClassicTreeBranch
				var hasProof bool

				proof, hasProof = parent.Proofs[current.Hash]

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
		// For chunked files, concatenate content from all chunks
		for _, link := range leaf.Links {
			childLeaf := dag.Leafs[link]
			if childLeaf == nil {
				return nil, fmt.Errorf("invalid link: %s", link)
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

// findParent searches the DAG for a leaf's parent
func (d *Dag) findParent(leaf *DagLeaf) *DagLeaf {
	for _, potential := range d.Leafs {
		if potential.HasLink(leaf.Hash) {
			return potential
		}
	}
	return nil
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

// GetLeafSequence returns an ordered sequence of leaves for transmission
// Each packet contains a leaf, its parent hash, and any proofs needed for verification
func (d *Dag) GetLeafSequence() []*TransmissionPacket {
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
