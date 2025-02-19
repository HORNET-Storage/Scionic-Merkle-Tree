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

	merkle_tree "github.com/HORNET-Storage/scionic-merkletree/tree"
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

func (dag *Dag) Verify() error {
	err := dag.IterateDag(func(leaf *DagLeaf, parent *DagLeaf) error {
		if leaf.Hash == dag.Root {
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

	if err != nil {
		return err
	}

	return nil
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
	branch := &DagBranch{
		Leaf:         leaf.Clone(),
		Path:         make([]*DagLeaf, 0),
		MerkleProofs: make(map[string]*ClassicTreeBranch),
	}

	// Find path to root through parent nodes
	current := leaf
	for current.Hash != d.Root {
		parent := d.findParent(current)
		if parent == nil {
			return nil, fmt.Errorf("failed to find parent for leaf %s", current.Hash)
		}

		// If parent has merkle tree, get proof
		if len(parent.Links) > 1 {
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

			proof, err := parent.GetBranch(label)
			if err != nil {
				return nil, err
			}
			branch.MerkleProofs[parent.Hash] = proof
		}

		branch.Path = append(branch.Path, parent.Clone())
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

	// Add path nodes and their merkle proofs
	for _, pathNode := range branch.Path {
		if _, exists := partial.Leafs[pathNode.Hash]; !exists {
			partial.Leafs[pathNode.Hash] = pathNode

			// If node has merkle proof, verify and include required siblings
			if proof, hasProof := branch.MerkleProofs[pathNode.Hash]; hasProof {
				err := pathNode.VerifyBranch(proof)
				if err != nil {
					return fmt.Errorf("invalid merkle proof for node %s: %w", pathNode.Hash, err)
				}
			}
		}
	}

	return nil
}

// GetPartial returns a new DAG containing only the requested leaves and their verification paths
func (d *Dag) GetPartial(start, end int) (*Dag, error) {
	partialDag := &Dag{
		Leafs: make(map[string]*DagLeaf),
		Root:  d.Root,
	}

	// Get root leaf
	rootLeaf := d.Leafs[d.Root].Clone()
	partialDag.Leafs[d.Root] = rootLeaf

	// Process each requested leaf
	for i := start; i <= end; i++ {
		label := strconv.Itoa(i)

		// Find and validate leaf
		var targetLeaf *DagLeaf
		for _, leaf := range d.Leafs {
			if GetLabel(leaf.Hash) == label {
				targetLeaf = leaf
				break
			}
		}

		if targetLeaf == nil {
			continue
		}

		// Build verification path
		branch, err := d.buildVerificationBranch(targetLeaf)
		if err != nil {
			return nil, err
		}

		// Add branch to partial DAG
		err = d.addBranchToPartial(branch, partialDag)
		if err != nil {
			return nil, err
		}
	}

	return partialDag, nil
}

// VerifyPartial verifies the integrity of a partial DAG
func (d *Dag) VerifyPartial() error {
	// Verify each leaf can trace to root with valid proofs
	for _, leaf := range d.Leafs {
		if leaf.Hash == d.Root {
			continue
		}

		current := leaf
		for current.Hash != d.Root {
			parent := d.findParent(current)
			if parent == nil {
				return fmt.Errorf("broken path to root for leaf %s", leaf.Hash)
			}

			// Verify merkle proof if parent has multiple children
			if len(parent.Links) > 1 {
				// Find the label for current in parent's links
				var label string
				for l, h := range parent.Links {
					if h == current.Hash {
						label = l
						break
					}
				}
				if label == "" {
					return fmt.Errorf("unable to find label for key")
				}

				branch, err := parent.GetBranch(label)
				if err != nil {
					return err
				}

				err = parent.VerifyBranch(branch)
				if err != nil {
					return fmt.Errorf("invalid merkle proof for node %s: %w", current.Hash, err)
				}
			}

			current = parent
		}
	}

	return nil
}
