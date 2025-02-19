![nostr Badge](https://img.shields.io/badge/nostr-8e30eb?style=flat) ![Go Badge](https://img.shields.io/badge/Go-00ADD8?logo=go&logoColor=white) <img src="https://static.wixstatic.com/media/e9326a_3823e7e6a7e14488954bb312d11636da~mv2.png" height="20"> ![example workflow](https://github.com/HORNET-Storage/scionic-merkletree/actions/workflows/go.yml/badge.svg)
[![codecov](https://codecov.io/gh/HORNET-Storage/scionic-merkletree/graph/badge.svg?token=1UBLJ1YYFI)](https://codecov.io/gh/HORNET-Storage/scionic-merkletree)


# Scionic Merkle Trees

## Combining Merkle Trees and Merkle DAGs

We've designed a [new type of Merkle DAG/Merkle Tree hybrid](https://www.hornet.storage/) known as the Scionic Merkle Tree. Scionic Merkle Trees contain small branches like Classic Merkle Trees, the folder storage support of Merkle DAGs, and numbered Merkle leaves so anyone can sync by requesting a range of missing leaf numbers that correspond to missing file chunks. LeafSync is the name of the simple protocol used to request a range of leaf numbers in order to retrieve a batch of missing file chunks corresponding to the leaf numbers.

![Tree Comparison Diagram](https://static.wixstatic.com/media/e9326a_ee5ee567806d439b93eaf3ce49afe072~mv2.png)

Scionic Merkle Trees maintain the advantages of IPFS Merkle DAGs with the slim Merkle Branches of Classic Merkle Trees, while providing LeafSync as a new feature that complements any set reconciliation system (IBLTs, negentropy, et al.). In plant grafting, the "Scion" is the upper part of the plant, chosen for its desirable fruits or flowers; it's grafted onto another plant's base to grow together. In a similar vein, the Scionic Merkle Tree was born from grafting together Merkle Trees and Merkle DAGs. This process emphasizes why we use the term "Scion" for the Scionic Merkle Trees: it symbolizes the digital grafting of these two similar data structures, combining their strengths into one piece of software.

## Scionic Merkle Trees: The Best of Both Worlds

### ***Classic Merkle Trees***

 Merkle Trees are cryptographic structures used to manage and securely verify large amounts of data. However, there's a significant drawback: they cannot store folders of files.

The number of hashes required for a Merkle proof in a Classic Merkle Tree grows logarithmically with the number of file chunks, meaning the growth rate slows as the input (tree) size increases. This pattern makes them very efficient for large datasets because the growth of the Merkle branch size becomes exponentially less as the number of chunks rise.

### ***Scionic Merkle Trees v.s IPFS Merkle DAGs***

Merkle DAGs were developed as a solution to incorporate folders of files, addressing a key limitation of Classic Merkle Trees. However, this structure has its own challenge: to securely download a single file chunk, you must download the hash of every other file chunk inside the folder its stored in. This means that each parent leaf can continue to grow if the number of file chunks in the folder grow, even though the size of each Merkle chunk should always remain the same! This flaw of parent leaves in IPFS Merkle DAGs is resolved by Scionic Merkle Trees because each Scionic parent leaf is chunked using a Classic Merkle Tree, ensuring every part of the Scionic Merkle Tree is uniformly chunked. In the most extreme cases of P2P decentralization, a user could retrieve each Merkle branch from a different source without needing to download the entire parent leaf first.

### ***Folders and Subfolders of Files:***

Like Merkle DAGs, Scionic Merkle Trees can accommodate storing folders of files. This means an entire directory of files and subfolders can be converted into a Scionic Merkle Tree.

### ***Chunked Parent Leaves:***

Within each parent leaf (folder), its list of hashes (chunks/children) are organized as a Classic Merkle Tree rather than a potentially large plaintext list of hashes. Large files or folders lead to many chunks, which can eventually lead to an extremely large lists of hashes. By ensuring the parent leaf is chunked with a Classic Merkle Tree, this scaling problem emerging from large amounts of data can be avoided.

### ***File Chunk Downloading with Chunked Parent Leaves:***

If a user wants to download a specific file chunk within a Scionic Merkle Tree, they no longer need to download every file chunk hash in its folder. Instead, they will download a Classic Merkle branch linked to the folder (parent leaf) they're downloading the file chunk from. This process allows the user to verify that the file is part of the tree without needing to download every hash of all other file chunks in the folder.

### ***Scionic Merkle Tree:***
![Scionic Merkle Tree Diagram](https://i.ibb.co/XJjbwmP/Scionic-Merkle-Tree.jpg)

### ***Scionic Merkle Branch:***
![Scionic Merkle Branch Diagram](https://i.ibb.co/nLcNLw1/Merkle-Branch.png)

## Scionic Merkle Branch Statistics

*Comparing the size of a Scionic Merkle Branch to bloated Merkle DAG Branches:*

* For a folder containing 10 files, a Scionic branch needs just 5 leaves, while a Merkle DAG branch requires all 10. This makes the Scionic branch about **2x smaller**.
* When the folder contains 1000 files, a Scionic branch uses only 11 leaves, compared to the full 1000 required by a Merkle DAG branch. This results in the Scionic branch being approximately **90x smaller**.
* In the case of a folder with 10,000 files, a Scionic branch requires 15 leaves, while a Merkle DAG branch needs all 10,000. This means the Scionic branch is roughly **710x smaller**.
* If the folder contains 1,000,000 files, a Scionic branch for any file in that folder would require around 21 leaves. This Scionic branch would be **50,000x smaller**.

These statistics underline the substantial efficiency improvements made by Scionic Merkle Trees.

## Understanding Growth Patterns: Logarithmic vs Linear

Scionic Merkle Trees, which incorporate Classic Merkle Trees within their structure, exhibit Merkle branches that grow logarithmically; this means that as the size of the input (the number of file chunks in a folder) increases, the growth rate of its Classic Merkle Tree branches decrease. This makes Scionic Merkle Trees an efficient structure for tranmissing large files ***because the growth of the Scionic Merkle branch becomes exponentially less*** as the number of file chunks increase, thanks to the Classic Merkle Trees nested within them.

In stark contrast, the number of hashes required to validate a single file chunk in an IPFS Merkle DAG exhibits linear growth. The hash of each file chunk in the folder must be downloaded in order to retrieve any individual file chunk from the folder. If the number of file chunks grow, then the parent leaf in the Merkle branch grows linearly in size as well; this requirement can lead to overly large Merkle branches that make IPFS Merkle DAGs less efficient for large datasets when compared to Scionic Merkle Trees.

## Syncing Scionic Merkle Trees by Requesting a Range of Leaf Numbers

To further enhance the functionality of Scionic Merkle Trees and support efficient data retrieval, each leaf in the tree is labeled with a sequenced number. The total number of leaves are listed within the Merkle root of the tree, meaning it must be downloaded first before the leaves can be retrieved. This method facilitates [LeafSync Messages, which are requests for a range of Merkle leaves](https://www.hornet.storage/negentropy-leafsync) that correspond to file chunks the requestor is missing.

# Documentation

## Install
```
go get github.com/HORNET-Storage/scionic-merkletree/dag
```

## Example Usage
There are good examples inside the dag/dag_test.go file, but below is a basic example to get you started. This library is intended to be very simple while still allowing for powerful usage...

Turn a folder and its files into a Scionic Merkle DAG-Tree, verify, then convert the Scionic Merkle tree back to the original files in a new directory:
```go
input := filepath.Join(tmpDir, "input")
output := filepath.Join(tmpDir, "output")

SetChunkSize(4096)

dag, err := CreateDag(input, true)
if err != nil {
  fmt.Fatalf("Error: %s", err)
}

result, err := dag.Verify()
if err != nil {
  fmt.Fatalf("Error: %s", err)
}

fmt.Println("Dag verified successfully")

err = dag.CreateDirectory(output)
if err != nil {
  fmt.Fatalf("Error: %s", err)
}
```

## Types

The dag builder and dag leaf builder types are used to temporarily store data during the dag creation process as the dag is created from the root down but then built from the bottom back up to the root.
It is not required to understand how this works but if you plan to build the trees yourself without the built in creation process (for example you may wish to create trees from data already in memory) then these will be useful.

### Dag Leaf
```go
type DagLeaf struct {
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
	ParentHash        string
	AdditionalData    map[string]string
}
```

Every leaf in the tree consists of the DagLeaf data type and these are what they are used for:

### Hash: string
The hash field is a cid, encoded as a string, of the following fields serialized in cbor with sha256 hasing:
- ItemName
- Type
- ContentHash
- ClassicMerkleRoot
- CurrentLinkCount
- AdditionalData

Only the root leaf has these fields included in the hash
- LatestLabel
- LeafCount

### ItemName: string
This can be anything but our usage is the file name including the type so that we can accurately re-create a directory / file with all the files and types intact

### Type: LeafType
This is a string but we use a custom type to enforce specific usage, there are currently only 3 types that a leaf can be:
```go
type LeafType string

const (
	FileLeafType      LeafType = "file"
	ChunkLeafType     LeafType = "chunk"
	DirectoryLeafType LeafType = "directory"
)
```

file is a file
chunk are the chunks that make up a file incase the file was larger than the max chunk size
directory is a directory

New types can be added without breaking existing data if needed

### ContentHash: []byte
### content: []byte
ContentHash and Content are important together as you can't have one without the other.
The content hash is a sha256 hash of the content, currently the content is from a file on disk but it could be anything as long as it's serialized in a byte array.
We have no need to encode any of this data as we are using cbor for serializing the leaf data which can safely handle byte arrays directly as it's not a plain text format like json.
The content hash is included in the leaf hash which means it's cryptographically verifiable, which also means the content can be verified as well to ensure there isn't tampering.
This is important because it means we can send and recieve the leaves with or without the content, while still being able to verify the content, which is important for de-duplicating data transmission over the network.

### ClassicMerkleRoot: []byte
We use classic merkle trees inside of our dag by creating a tree of the links inside of a leaf, if the leaf has more than 1 link. This allows us to verify the leaves without having all of the children present making our branches a lot smaller.
This also means we do not need to include the links in the leaf hash because this merkle root is included in their place, potentially removing a lot of data when sending individual leaves if there are a lot of child leaves present.

### CurrentLinkCount: int
This is the count of how many links a leaf has and it's included in the leaf hash to ensure that we always know and can verify how many links a leaf should have which prevents any lying about the number of children when verifying branches or partial trees.

### LatestLabel: string
We label every child leaf in the dag where the root starts at 0 and each leaf that gets built becomes the next integer. Because these are included in the classic merkle tree, and the classic merkle root is included in the leaf hash, we can now reference leaves by their root hash and number instead of their root hash and their leaf hash.
This is stored as a string as it is appended to the cid (Hash) of each leaf. It's important to remember that labelling is not per leaf but per dag.

### LeafCount: int
The overall number of leaves that the entire dag contains which is why this is only stored and hashed in the root leaf, it ensures you can always know if you have all of the children or not.

### Links: map[string]string
The links to all of the children of a leaf where the key is the label and the value is the label:cid of the child

### ParentHash: string
We add the parent hash (label:cid) to the child leaf to make traversal upwards possible but this is purely for speed and the parent it points to should still be verified as we can't include the parent hash inside of the leaf hash.
This is because the parent hash doesn't exist yet, the leaf hashes are created from bottom to top, despite dag creation starting at the top.

### AdditionalData: map[string]string
This map is included in the leaf hash allowing for developers to add additional data to the dag leaves if and when needed.
AdditionalData does get included in the leaf hash so any content stored here is cryptographically verifiable, the map is sorted by keys alphanumerically before it gets serialized and hashed to ensure consistency no matter what order they get added.
Currently we only use this to store the timestamp in the root leaf which is an optional parameter when creating a dag from a directory or file but advanced users that build the trees themselves can utilize this feature to store anything they want.

## Functions
```go
func CreateDagBuilder() *DagBuilder
func (b *DagBuilder) AddLeaf(leaf *DagLeaf, parentLeaf *DagLeaf) error
func (b *DagBuilder) BuildDag(root string) *Dag
func (b *DagBuilder) GetLatestLabel()
func (b *DagBuilder) GetNextAvailableLabel()

func CreateDag(path string, timestampRoot bool) (*Dag, error)
func (dag *Dag) Verify() error
func (dag *Dag) CreateDirectory(path string) error
func (dag *Dag) GetContentFromLeaf(leaf *DagLeaf) ([]byte, error)
func (dag *Dag) IterateDag(processLeaf func(leaf *DagLeaf, parent *DagLeaf) error) error

func CreateDagLeafBuilder(name string) *DagLeafBuilder
func (b *DagLeafBuilder) SetType(leafType LeafType) 
func (b *DagLeafBuilder) SetData(data []byte)
func (b *DagLeafBuilder) AddLink(label string, hash string) 
func (b *DagLeafBuilder) BuildLeaf(additionalData map[string]string) (*DagLeaf, error) 
func (b *DagLeafBuilder) BuildRootLeaf(dag *DagBuilder, additionalData map[string]string) (*DagLeaf, error)

func (leaf *DagLeaf) GetBranch(key string) (*ClassicTreeBranch, error)
func (leaf *DagLeaf) VerifyBranch(branch *ClassicTreeBranch) error
func (leaf *DagLeaf) VerifyLeaf() error
func (leaf *DagLeaf) VerifyRootLeaf() error
func (leaf *DagLeaf) CreateDirectoryLeaf(path string, dag *Dag) error
func (leaf *DagLeaf) HasLink(hash string) bool
func (leaf *DagLeaf) AddLink(hash string)
func (leaf *DagLeaf) Clone() *DagLeaf
func (leaf *DagLeaf) SetLabel(label string)
```

The trees are now in beta and the data structure of the trees will no longer change.
#
