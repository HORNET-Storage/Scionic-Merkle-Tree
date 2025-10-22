package diff

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/HORNET-Storage/Scionic-Merkle-Tree/dag"
)

func TestDiffFromNewLeaves(t *testing.T) {
	// Create a temporary test directory
	testDir, err := os.MkdirTemp("", "dag_diff_from_leaves_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Create initial directory structure
	dir1 := filepath.Join(testDir, "input1")
	if err := os.MkdirAll(dir1, 0755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir1, "file1.txt"), []byte("content1"), 0644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	// Create old DAG
	oldDag, err := dag.CreateDag(dir1, false)
	if err != nil {
		t.Fatalf("Failed to create old DAG: %v", err)
	}

	// Add more files
	if err := os.WriteFile(filepath.Join(dir1, "file2.txt"), []byte("content2"), 0644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	// Create new DAG
	newDag, err := dag.CreateDag(dir1, false)
	if err != nil {
		t.Fatalf("Failed to create new DAG: %v", err)
	}

	// Use DiffFromNewLeaves
	diff, err := DiffFromNewLeaves(oldDag, newDag.Leafs)
	if err != nil {
		t.Fatalf("Failed to create diff from new leaves: %v", err)
	}

	// Should have some added leaves
	if diff.Summary.Added == 0 {
		t.Error("Expected some added leaves")
	}

	t.Logf("DiffFromNewLeaves results: Added=%d, Removed=%d, Total=%d",
		diff.Summary.Added, diff.Summary.Removed, diff.Summary.Total)
}

func TestGetAddedLeaves(t *testing.T) {
	// Create temporary test directories
	testDir, err := os.MkdirTemp("", "dag_get_added_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Create two different directory structures
	dir1 := filepath.Join(testDir, "input1")
	dir2 := filepath.Join(testDir, "input2")

	if err := os.MkdirAll(dir1, 0755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	if err := os.MkdirAll(dir2, 0755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}

	if err := os.WriteFile(filepath.Join(dir1, "file1.txt"), []byte("content1"), 0644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir2, "file1.txt"), []byte("content1"), 0644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir2, "file2.txt"), []byte("content2"), 0644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	dag1, err := dag.CreateDag(dir1, false)
	if err != nil {
		t.Fatalf("Failed to create first DAG: %v", err)
	}

	dag2, err := dag.CreateDag(dir2, false)
	if err != nil {
		t.Fatalf("Failed to create second DAG: %v", err)
	}

	// Create diff
	diff, err := Diff(dag1, dag2)
	if err != nil {
		t.Fatalf("Failed to create diff: %v", err)
	}

	// Get added leaves
	addedLeaves := diff.GetAddedLeaves()

	if len(addedLeaves) == 0 {
		t.Error("Expected some added leaves")
	}

	// Verify all returned leaves are actually in the added diffs
	for bareHash := range addedLeaves {
		leafDiff, exists := diff.Diffs[bareHash]
		if !exists {
			t.Errorf("Added leaf %s not found in diff", bareHash)
		}
		if leafDiff.Type != DiffTypeAdded {
			t.Errorf("Leaf %s has type %s, expected %s", bareHash, leafDiff.Type, DiffTypeAdded)
		}
	}

	t.Logf("GetAddedLeaves returned %d leaves", len(addedLeaves))
}

func TestGetRemovedLeaves(t *testing.T) {
	// Create temporary test directories
	testDir, err := os.MkdirTemp("", "dag_get_removed_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Create two different directory structures
	dir1 := filepath.Join(testDir, "input1")
	dir2 := filepath.Join(testDir, "input2")

	if err := os.MkdirAll(dir1, 0755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	if err := os.MkdirAll(dir2, 0755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}

	if err := os.WriteFile(filepath.Join(dir1, "file1.txt"), []byte("content1"), 0644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir1, "file2.txt"), []byte("content2"), 0644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir2, "file1.txt"), []byte("content1"), 0644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	dag1, err := dag.CreateDag(dir1, false)
	if err != nil {
		t.Fatalf("Failed to create first DAG: %v", err)
	}

	dag2, err := dag.CreateDag(dir2, false)
	if err != nil {
		t.Fatalf("Failed to create second DAG: %v", err)
	}

	// Create diff
	diff, err := Diff(dag1, dag2)
	if err != nil {
		t.Fatalf("Failed to create diff: %v", err)
	}

	// Get removed leaves
	removedLeaves := diff.GetRemovedLeaves()

	if len(removedLeaves) == 0 {
		t.Error("Expected some removed leaves")
	}

	// Verify all returned leaves are actually in the removed diffs
	for bareHash := range removedLeaves {
		leafDiff, exists := diff.Diffs[bareHash]
		if !exists {
			t.Errorf("Removed leaf %s not found in diff", bareHash)
		}
		if leafDiff.Type != DiffTypeRemoved {
			t.Errorf("Leaf %s has type %s, expected %s", bareHash, leafDiff.Type, DiffTypeRemoved)
		}
	}

	t.Logf("GetRemovedLeaves returned %d leaves", len(removedLeaves))
}

func TestApplyToDAG(t *testing.T) {
	// Create temporary test directory
	testDir, err := os.MkdirTemp("", "dag_apply_diff_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Create initial directory
	dir1 := filepath.Join(testDir, "input1")
	if err := os.MkdirAll(dir1, 0755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir1, "file1.txt"), []byte("content1"), 0644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	// Create old DAG
	oldDag, err := dag.CreateDag(dir1, false)
	if err != nil {
		t.Fatalf("Failed to create old DAG: %v", err)
	}

	oldLeafCount := len(oldDag.Leafs)

	// Modify directory
	if err := os.WriteFile(filepath.Join(dir1, "file2.txt"), []byte("content2"), 0644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	// Create new DAG
	newDag, err := dag.CreateDag(dir1, false)
	if err != nil {
		t.Fatalf("Failed to create new DAG: %v", err)
	}

	newLeafCount := len(newDag.Leafs)

	// Create diff
	diff, err := Diff(oldDag, newDag)
	if err != nil {
		t.Fatalf("Failed to create diff: %v", err)
	}

	// Apply diff to old DAG
	reconstructedDag, err := diff.ApplyToDAG(oldDag)
	if err != nil {
		t.Fatalf("Failed to apply diff: %v", err)
	}

	// Verify reconstructed DAG has same number of leaves as new DAG
	if len(reconstructedDag.Leafs) != newLeafCount {
		t.Errorf("Reconstructed DAG has %d leaves, expected %d", len(reconstructedDag.Leafs), newLeafCount)
	}

	// Verify all leaves from new DAG exist in reconstructed DAG
	for newHash, newLeaf := range newDag.Leafs {
		found := false
		for reconHash, reconLeaf := range reconstructedDag.Leafs {
			if reconHash == newHash {
				found = true
				// Verify the leaf content matches
				if reconLeaf.ItemName != newLeaf.ItemName {
					t.Errorf("Leaf %s has different ItemName: got %s, expected %s",
						newHash, reconLeaf.ItemName, newLeaf.ItemName)
				}
				break
			}
		}
		if !found {
			t.Errorf("Leaf %s from new DAG not found in reconstructed DAG", newHash)
		}
	}

	// Verify reconstructed DAG can be verified
	err = reconstructedDag.Verify()
	if err != nil {
		t.Errorf("Reconstructed DAG failed verification: %v", err)
	}

	t.Logf("Successfully applied diff: %d -> %d leaves", oldLeafCount, len(reconstructedDag.Leafs))
	t.Logf("Added: %d, Removed: %d", diff.Summary.Added, diff.Summary.Removed)
}

func TestCreatePartialDAGFromAdded(t *testing.T) {
	// Create temporary test directories
	testDir, err := os.MkdirTemp("", "dag_partial_from_diff_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Create two directory structures
	dir1 := filepath.Join(testDir, "input1")
	dir2 := filepath.Join(testDir, "input2")

	if err := os.MkdirAll(dir1, 0755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	if err := os.MkdirAll(dir2, 0755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}

	if err := os.WriteFile(filepath.Join(dir1, "file1.txt"), []byte("content1"), 0644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir2, "file1.txt"), []byte("content1"), 0644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir2, "file2.txt"), []byte("content2"), 0644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	dag1, err := dag.CreateDag(dir1, false)
	if err != nil {
		t.Fatalf("Failed to create first DAG: %v", err)
	}

	dag2, err := dag.CreateDag(dir2, false)
	if err != nil {
		t.Fatalf("Failed to create second DAG: %v", err)
	}

	// Create diff
	diff, err := Diff(dag1, dag2)
	if err != nil {
		t.Fatalf("Failed to create diff: %v", err)
	}

	// Create partial DAG from added leaves using the full new DAG
	partialDag, err := diff.CreatePartialDag(dag2)
	if err != nil {
		t.Fatalf("Failed to create partial DAG: %v", err)
	}

	// Verify partial DAG contains at least the added file leaves
	// Note: The partial DAG may have MORE leaves than just the added leaves
	// because it includes verification paths and intermediate directories with Merkle proofs
	addedLeaves := diff.GetAddedLeaves()

	// Count added file leaves
	addedFileCount := 0
	for _, leaf := range addedLeaves {
		if leaf.Type == dag.FileLeafType {
			addedFileCount++
		}
	}

	// Verify all added file leaves are in the partial DAG
	for hash, addedLeaf := range addedLeaves {
		if addedLeaf.Type == dag.FileLeafType {
			found := false
			for partialHash := range partialDag.Leafs {
				if partialHash == hash {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Partial DAG missing added file leaf %s (name: %s)", hash[:16], addedLeaf.ItemName)
			}
		}
	}

	// Verify partial DAG has a valid root
	if partialDag.Root == "" {
		t.Error("Partial DAG has no root")
	}

	rootLeaf, exists := partialDag.Leafs[partialDag.Root]
	if !exists {
		t.Error("Partial DAG root not found in leaves")
	} else {
		t.Logf("Partial DAG root: %s (type: %s, name: %s)",
			partialDag.Root, rootLeaf.Type, rootLeaf.ItemName)
	}

	t.Logf("Created partial DAG with %d leaves from %d added leaves",
		len(partialDag.Leafs), len(addedLeaves))
}

func TestCompleteWorkflow_NetworkTransmission(t *testing.T) {
	// This test simulates a complete network transmission workflow
	// The key insight: we're NOT trying to reconstruct the exact new DAG structure.
	// We're just merging the received leaves with our old DAG.
	// The new DAG structure will be different (because the receiver builds it from their perspective)
	// but all the content (leaves) should be present.

	// Create temporary test directory
	testDir, err := os.MkdirTemp("", "dag_workflow_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	// === SENDER SIDE ===

	// Sender has old state
	oldDir := filepath.Join(testDir, "old")
	if err := os.MkdirAll(oldDir, 0755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(oldDir, "file1.txt"), []byte("original"), 0644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	senderOldDag, err := dag.CreateDag(oldDir, false)
	if err != nil {
		t.Fatalf("Failed to create sender old DAG: %v", err)
	}

	// Sender creates new state
	if err := os.WriteFile(filepath.Join(oldDir, "file2.txt"), []byte("new content"), 0644); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	senderNewDag, err := dag.CreateDag(oldDir, false)
	if err != nil {
		t.Fatalf("Failed to create sender new DAG: %v", err)
	}

	// Sender creates diff
	diff, err := Diff(senderOldDag, senderNewDag)
	if err != nil {
		t.Fatalf("Failed to create diff: %v", err)
	}

	t.Logf("Sender created diff: Added=%d, Removed=%d",
		diff.Summary.Added, diff.Summary.Removed)

	// Sender creates partial DAG for transmission (only added leaves with verification paths)
	partialDag, err := diff.CreatePartialDag(senderNewDag)
	if err != nil {
		t.Fatalf("Failed to create partial DAG: %v", err)
	}

	t.Logf("Sender created partial DAG with %d leaves for transmission", len(partialDag.Leafs))

	// === NETWORK TRANSMISSION ===
	// In reality, partialDag would be serialized and sent over the network
	// For this test, we just pass it directly

	// === RECEIVER SIDE ===

	// Receiver has the old DAG (same as sender's old DAG in this test)
	receiverOldDag := senderOldDag

	// Receiver gets the new leaves from partial DAG
	newLeaves := partialDag.Leafs

	// Receiver creates diff from new leaves
	receiverDiff, err := DiffFromNewLeaves(receiverOldDag, newLeaves)
	if err != nil {
		t.Fatalf("Failed to create receiver diff: %v", err)
	}

	t.Logf("Receiver created diff from received leaves: Added=%d, Removed=%d",
		receiverDiff.Summary.Added, receiverDiff.Summary.Removed)

	// Receiver applies diff to create new DAG
	receiverNewDag, err := receiverDiff.ApplyToDAG(receiverOldDag)
	if err != nil {
		t.Fatalf("Failed to apply diff on receiver: %v", err)
	}

	t.Logf("Receiver reconstructed DAG with %d leaves", len(receiverNewDag.Leafs))
	t.Logf("Sender new DAG has %d leaves", len(senderNewDag.Leafs))

	// === VERIFICATION ===

	// The key verification: All content (bare hashes) that exists in sender's new DAG
	// should exist in receiver's new DAG. The structure might be different because
	// labels are recomputed, but all the actual content should be present.

	// Collect all bare hashes from sender's new DAG
	senderBareHashes := make(map[string]*dag.DagLeaf)
	for hash, leaf := range senderNewDag.Leafs {
		senderBareHashes[hash] = leaf
	}

	// Collect all bare hashes from receiver's new DAG
	receiverBareHashes := make(map[string]*dag.DagLeaf)
	for hash, leaf := range receiverNewDag.Leafs {
		receiverBareHashes[hash] = leaf
	}

	// Verify all content from sender exists in receiver
	for bareHash, senderLeaf := range senderBareHashes {
		receiverLeaf, exists := receiverBareHashes[bareHash]
		if !exists {
			t.Errorf("Receiver missing content %s (name: %s)", bareHash[:16], senderLeaf.ItemName)
			continue
		}
		if receiverLeaf.ItemName != senderLeaf.ItemName {
			t.Errorf("Content %s name mismatch: receiver=%s, sender=%s",
				bareHash[:16], receiverLeaf.ItemName, senderLeaf.ItemName)
		}
		if receiverLeaf.Type != senderLeaf.Type {
			t.Errorf("Content %s type mismatch: receiver=%s, sender=%s",
				bareHash[:16], receiverLeaf.Type, senderLeaf.Type)
		}
	}

	// Verify receiver DAG validates
	err = receiverNewDag.Verify()
	if err != nil {
		t.Errorf("Receiver DAG failed verification: %v", err)
	}

	// Verify both DAGs have the same number of unique content pieces
	if len(senderBareHashes) != len(receiverBareHashes) {
		t.Errorf("Content count mismatch: sender has %d unique pieces, receiver has %d",
			len(senderBareHashes), len(receiverBareHashes))
	}

	t.Logf("✓ Complete workflow successful - all content transmitted and verified")
}

func TestApplyToDAG_EmptyDiff(t *testing.T) {
	testDir, err := os.MkdirTemp("", "dag_apply_empty_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Create deterministic input directory
	dir := filepath.Join(testDir, "input")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("Failed to create input directory: %v", err)
	}

	// Create a simple file
	if err := os.WriteFile(filepath.Join(dir, "test.txt"), []byte("test content"), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	dag, err := dag.CreateDag(dir, false)
	if err != nil {
		t.Fatalf("Failed to create DAG: %v", err)
	}

	// Create empty diff (compare dag with itself)
	diff, err := Diff(dag, dag)
	if err != nil {
		t.Fatalf("Failed to create diff: %v", err)
	}

	// Apply empty diff
	resultDag, err := diff.ApplyToDAG(dag)
	if err != nil {
		t.Fatalf("Failed to apply empty diff: %v", err)
	}

	// Should have same number of leaves
	if len(resultDag.Leafs) != len(dag.Leafs) {
		t.Errorf("Result DAG has %d leaves, original has %d",
			len(resultDag.Leafs), len(dag.Leafs))
	}
}

func TestCreatePartialDAGFromAdded_NoAdded(t *testing.T) {
	testDir, err := os.MkdirTemp("", "dag_partial_no_added_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Create deterministic input directory
	dir := filepath.Join(testDir, "input")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("Failed to create input directory: %v", err)
	}

	// Create a simple file
	if err := os.WriteFile(filepath.Join(dir, "test.txt"), []byte("test content"), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	dag, err := dag.CreateDag(dir, false)
	if err != nil {
		t.Fatalf("Failed to create DAG: %v", err)
	}

	// Create diff with no additions
	diff, err := Diff(dag, dag)
	if err != nil {
		t.Fatalf("Failed to create diff: %v", err)
	}

	// Try to create partial DAG from empty additions (pass the same DAG since there are no changes)
	_, err = diff.CreatePartialDag(dag)
	if err == nil {
		t.Error("Expected error when creating partial DAG with no added leaves")
	}
}
