package dag

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"time"
)

func GenerateDummyDirectory(path string, minItems, maxItems, minDepth, maxDepth int) {
	rand.Seed(time.Now().UnixNano())

	err := createRandomDirsAndFiles(path, minDepth, maxDepth, minItems, maxItems)
	if err != nil {
		fmt.Println("Error:", err)
	}
}

func createRandomDirsAndFiles(path string, minDepth, depth, minItems, maxItems int) error {
	if depth == 0 {
		return nil
	}

	// Create directory if it doesn't exist
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.Mkdir(path, 0755); err != nil {
			return err
		}
	}

	// Ensure we meet minimum items
	numItems := minItems
	if maxItems > minItems {
		numItems += rand.Intn(maxItems - minItems)
	}

	// If we're at minDepth or above, ensure at least one subdirectory
	needSubdir := depth > minDepth

	for i := 0; i < numItems; i++ {
		if needSubdir || rand.Intn(2) == 0 {
			subDir := fmt.Sprintf("%s/subdir%d", path, i)
			err := createRandomDirsAndFiles(subDir, minDepth, depth-1, minItems, maxItems)
			needSubdir = false // We've created our required subdir
			if err != nil {
				return err
			}
		} else {
			filePath := fmt.Sprintf("%s/file%d.txt", path, i)
			randomData := make([]byte, rand.Intn(100))
			rand.Read(randomData)
			if err := ioutil.WriteFile(filePath, randomData, 0644); err != nil {
				return err
			}
		}
	}
	return nil
}

func FindRandomChild(leaf *DagLeaf, leafs map[string]*DagLeaf) *DagLeaf {
	if leaf.Type == DirectoryLeafType && len(leaf.Links) > 0 {
		rand.Seed(time.Now().UnixNano())

		// Get all links in a sorted slice
		var labels []string
		for label := range leaf.Links {
			labels = append(labels, label)
		}
		sort.Strings(labels)

		// Pick a random label
		randomLabel := labels[rand.Intn(len(labels))]
		link := leaf.Links[randomLabel]

		childLeaf := leafs[link].Clone()
		// Preserve merkle tree data
		if len(childLeaf.Links) > 1 {
			originalLinks := childLeaf.Links
			childLeaf.Links = make(map[string]string)
			for k, v := range originalLinks {
				childLeaf.Links[k] = v
			}
		}
		return childLeaf
	}

	return leaf
}

func CreateDummyLeaf(name string) (*DagLeaf, error) {
	rand.Seed(time.Now().UnixNano())

	builder := CreateDagLeafBuilder(name)

	builder.SetType(FileLeafType)

	data := make([]byte, rand.Intn(100)+10) // 10 to 100 bytes of random data
	rand.Read(data)

	chunkSize := 20
	var chunks [][]byte
	for i := 0; i < len(data); i += chunkSize {
		end := i + chunkSize
		if end > len(data) {
			end = len(data)
		}
		chunks = append(chunks, data[i:end])
	}

	if len(chunks) == 1 {
		builder.SetData(chunks[0])
	} else {
		for i, chunk := range chunks {
			chunkEntryName := fmt.Sprintf("%s_%d", name, i)
			chunkBuilder := CreateDagLeafBuilder(chunkEntryName)

			chunkBuilder.SetType(ChunkLeafType)
			chunkBuilder.SetData(chunk)

			chunkLeaf, err := chunkBuilder.BuildLeaf(nil)
			if err != nil {
				return nil, err
			}

			label := fmt.Sprintf("%d", i)
			builder.AddLink(label, chunkLeaf.Hash)
		}
	}

	return builder.BuildLeaf(nil)
}

func GenerateLargeDummyDirectory(path string, minItems, maxItems, minDepth, maxDepth int, fileSize int) {
	rand.Seed(time.Now().UnixNano())

	err := createRandomDirsAndFilesLarge(path, minDepth, maxDepth, minItems, maxItems, fileSize)
	if err != nil {
		fmt.Println("Error:", err)
	}
}

func createRandomDirsAndFilesLarge(path string, minDepth, depth, minItems, maxItems int, fileSize int) error {
	if depth == 0 {
		return nil
	}

	// Create directory if it doesn't exist
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.Mkdir(path, 0755); err != nil {
			return err
		}
	}

	// Ensure we meet minimum items
	numItems := minItems
	if maxItems > minItems {
		numItems += rand.Intn(maxItems - minItems)
	}

	// If we're at minDepth or above, ensure at least one subdirectory
	needSubdir := depth > minDepth

	for i := 0; i < numItems; i++ {
		if needSubdir || rand.Intn(2) == 0 {
			subDir := fmt.Sprintf("%s/subdir%d", path, i)
			err := createRandomDirsAndFilesLarge(subDir, minDepth, depth-1, minItems, maxItems, fileSize)
			needSubdir = false // We've created our required subdir
			if err != nil {
				return err
			}
		} else {
			filePath := fmt.Sprintf("%s/file%d.txt", path, i)
			randomData := make([]byte, fileSize)
			rand.Read(randomData)
			if err := ioutil.WriteFile(filePath, randomData, 0644); err != nil {
				return err
			}
		}
	}
	return nil
}

// GenerateMerkleProofTriggerDirectory creates a directory structure specifically designed
// to trigger the batched transmission Merkle proof bug. It creates directories with
// many children that will be batched together, requiring Merkle proof generation.
func GenerateMerkleProofTriggerDirectory(path string, numChildrenPerDir int, fileSize int) {
	rand.Seed(time.Now().UnixNano())

	err := createMerkleProofTriggerStructure(path, numChildrenPerDir, fileSize)
	if err != nil {
		fmt.Println("Error creating Merkle proof trigger directory:", err)
	}
}

func createMerkleProofTriggerStructure(path string, numChildrenPerDir int, fileSize int) error {
	// Create root directory
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.Mkdir(path, 0755); err != nil {
			return err
		}
	}

	// Create several subdirectories, each with many children
	// This ensures we have parents with multiple children that get batched together
	for dirIdx := 0; dirIdx < 5; dirIdx++ {
		subDir := fmt.Sprintf("%s/testdir%d", path, dirIdx)
		if err := os.Mkdir(subDir, 0755); err != nil {
			return err
		}

		// Create many files in each subdirectory
		// This creates the scenario where multiple children of the same parent
		// need to be batched together with Merkle proofs
		for fileIdx := 0; fileIdx < numChildrenPerDir; fileIdx++ {
			filePath := fmt.Sprintf("%s/file%d.txt", subDir, fileIdx)
			randomData := make([]byte, fileSize)
			rand.Read(randomData)
			if err := ioutil.WriteFile(filePath, randomData, 0644); err != nil {
				return err
			}
		}
	}

	// Create a nested structure to test hierarchical batching
	nestedDir := fmt.Sprintf("%s/nested", path)
	if err := os.Mkdir(nestedDir, 0755); err != nil {
		return err
	}

	for i := 0; i < 3; i++ {
		subNested := fmt.Sprintf("%s/sub%d", nestedDir, i)
		if err := os.Mkdir(subNested, 0755); err != nil {
			return err
		}

		// Fewer files in nested dirs to test different batching scenarios
		for j := 0; j < numChildrenPerDir/2; j++ {
			filePath := fmt.Sprintf("%s/nested_file%d.txt", subNested, j)
			randomData := make([]byte, fileSize)
			rand.Read(randomData)
			if err := ioutil.WriteFile(filePath, randomData, 0644); err != nil {
				return err
			}
		}
	}

	return nil
}

// GenerateMerkleProofTriggerDirectoryOutOfOrder creates a directory structure specifically designed
// to trigger the batched transmission Merkle proof bug. It creates directories with
// many children that will be batched together, requiring Merkle proof generation.
// This version uses specific filenames that will be out of alphabetical order.
func GenerateMerkleProofTriggerDirectoryOutOfOrder(path string, numChildrenPerDir int, fileSize int) {
	rand.Seed(time.Now().UnixNano())

	err := createMerkleProofTriggerStructureOutOfOrder(path, numChildrenPerDir, fileSize)
	if err != nil {
		fmt.Println("Error creating Merkle proof trigger directory:", err)
	}
}

func createMerkleProofTriggerStructureOutOfOrder(path string, numChildrenPerDir int, fileSize int) error {
	// Create root directory
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.Mkdir(path, 0755); err != nil {
			return err
		}
	}

	// Create several subdirectories, each with many children
	// Use filenames that will be out of alphabetical order to trigger the bug
	outOfOrderNames := []string{"z_file", "a_file", "m_file", "b_file", "y_file", "c_file", "x_file", "d_file", "w_file", "e_file"}

	for dirIdx := 0; dirIdx < 3; dirIdx++ {
		subDir := fmt.Sprintf("%s/testdir%d", path, dirIdx)
		if err := os.Mkdir(subDir, 0755); err != nil {
			return err
		}

		// Create many files in each subdirectory using out-of-order names
		// This creates the scenario where multiple children of the same parent
		// need to be batched together with Merkle proofs, and the labels will
		// be in a different order than the sorted keys
		for fileIdx := 0; fileIdx < numChildrenPerDir && fileIdx < len(outOfOrderNames); fileIdx++ {
			fileName := fmt.Sprintf("%s%d.txt", outOfOrderNames[fileIdx], fileIdx)
			filePath := fmt.Sprintf("%s/%s", subDir, fileName)
			randomData := make([]byte, fileSize)
			rand.Read(randomData)
			if err := ioutil.WriteFile(filePath, randomData, 0644); err != nil {
				return err
			}
		}
	}

	return nil
}
