package dag

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"time"
)

func GenerateDummyDirectory(path string, maxItems int, maxDepth int) {
	rand.Seed(time.Now().UnixNano())

	err := createRandomDirsAndFiles(path, maxDepth, maxItems)
	if err != nil {
		fmt.Println("Error:", err)
	}
}

func createRandomDirsAndFiles(path string, depth int, maxItems int) error {
	if depth == 0 {
		return nil
	}

	if _, err := os.Stat(path); os.IsNotExist(err) {
		err := os.Mkdir(path, 0755)
		if err != nil {
			return err
		}
	}

	numItems := rand.Intn(maxItems) + 1
	for i := 0; i < numItems; i++ {
		if rand.Intn(2) == 0 {
			subDir := fmt.Sprintf("%s/subdir%d", path, i)
			err := createRandomDirsAndFiles(subDir, depth-1, maxItems)
			if err != nil {
				return err
			}
		} else {
			filePath := fmt.Sprintf("%s/file%d.txt", path, i)
			randomData := make([]byte, rand.Intn(100))
			rand.Read(randomData)
			err := ioutil.WriteFile(filePath, randomData, 0644)
			if err != nil {
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
