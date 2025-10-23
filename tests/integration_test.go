package tests

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/HORNET-Storage/Scionic-Merkle-Tree/v2/dag"
	"github.com/HORNET-Storage/Scionic-Merkle-Tree/v2/diff"
)

// TestCompleteWorkflowDeepHierarchy tests the complete end-to-end workflow:
// 1. Create a deep hierarchy DAG (sender's old state)
// 2. Modify the directory (add/change files)
// 3. Create new DAG (sender's new state)
// 4. Create diff between old and new
// 5. Create partial DAG from diff (for transmission)
// 6. Transmit partial leaf-by-leaf with verification
// 7. Reconstruct partial from transmission packets
// 8. Verify reconstructed partial
// 9. Create diff on receiver side using old DAG + received partial
// 10. Apply diff to create new DAG on receiver side
// 11. Write receiver's new DAG to disk
// 12. Verify final DAG matches sender's new DAG
func TestCompleteWorkflowDeepHierarchy(t *testing.T) {
	// Create test directory structure
	testDir := t.TempDir()
	senderDir := filepath.Join(testDir, "sender")
	receiverDir := filepath.Join(testDir, "receiver")

	if err := os.MkdirAll(senderDir, 0755); err != nil {
		t.Fatalf("Failed to create sender directory: %v", err)
	}
	if err := os.MkdirAll(receiverDir, 0755); err != nil {
		t.Fatalf("Failed to create receiver directory: %v", err)
	}

	// ===================================================================
	// PHASE 1: CREATE INITIAL DEEP HIERARCHY (SENDER'S OLD STATE)
	// ===================================================================
	t.Log("PHASE 1: Creating initial deep hierarchy...")

	// Create a deep directory structure with multiple subdirectories and files
	oldPath := filepath.Join(senderDir, "old")
	if err := createDeepHierarchy(oldPath, 4, 3); err != nil {
		t.Fatalf("Failed to create initial hierarchy: %v", err)
	}

	// Create sender's old DAG
	senderOldDag, err := dag.CreateDag(oldPath, false)
	if err != nil {
		t.Fatalf("Failed to create sender old DAG: %v", err)
	}

	// Verify sender's old DAG
	if err := senderOldDag.Verify(); err != nil {
		t.Fatalf("Sender old DAG verification failed: %v", err)
	}

	t.Logf("✓ Sender old DAG created: %d leaves", len(senderOldDag.Leafs))

	// ===================================================================
	// PHASE 2: MODIFY DIRECTORY (ADD/CHANGE FILES)
	// ===================================================================
	t.Log("\nPHASE 2: Modifying directory structure...")

	// Add new files to existing directories
	newFile1 := filepath.Join(oldPath, "level1_0", "new_file_1.txt")
	if err := os.WriteFile(newFile1, []byte("This is a new file in level 1"), 0644); err != nil {
		t.Fatalf("Failed to write new file: %v", err)
	}

	newFile2 := filepath.Join(oldPath, "level1_0", "level2_0", "new_file_2.txt")
	if err := os.WriteFile(newFile2, []byte("This is a new file in level 2"), 0644); err != nil {
		t.Fatalf("Failed to write new file: %v", err)
	}

	// Modify existing file
	existingFile := filepath.Join(oldPath, "level1_0", "file_0.txt")
	if err := os.WriteFile(existingFile, []byte("MODIFIED CONTENT"), 0644); err != nil {
		t.Fatalf("Failed to modify existing file: %v", err)
	}

	// Create a new subdirectory with files
	newSubdir := filepath.Join(oldPath, "level1_0", "new_subdir")
	if err := os.MkdirAll(newSubdir, 0755); err != nil {
		t.Fatalf("Failed to create new subdirectory: %v", err)
	}
	newFile3 := filepath.Join(newSubdir, "nested_new_file.txt")
	if err := os.WriteFile(newFile3, []byte("File in new subdirectory"), 0644); err != nil {
		t.Fatalf("Failed to write nested file: %v", err)
	}

	t.Logf("✓ Modified directory: added 3 files, modified 1 file, created 1 subdirectory")

	// ===================================================================
	// PHASE 3: CREATE NEW DAG (SENDER'S NEW STATE)
	// ===================================================================
	t.Log("\nPHASE 3: Creating new DAG after modifications...")

	// Create sender's new DAG
	senderNewDag, err := dag.CreateDag(oldPath, false)
	if err != nil {
		t.Fatalf("Failed to create sender new DAG: %v", err)
	}

	// Verify sender's new DAG
	if err := senderNewDag.Verify(); err != nil {
		t.Fatalf("Sender new DAG verification failed: %v", err)
	}

	t.Logf("✓ Sender new DAG created: %d leaves", len(senderNewDag.Leafs))

	// ===================================================================
	// PHASE 4: CREATE DIFF BETWEEN OLD AND NEW DAGS
	// ===================================================================
	t.Log("\nPHASE 4: Creating diff between old and new DAGs...")

	d, err := diff.Diff(senderOldDag, senderNewDag)
	if err != nil {
		t.Fatalf("Failed to create diff: %v", err)
	}

	t.Logf("✓ Diff created: %d added, %d removed",
		d.Summary.Added, d.Summary.Removed)

	// VALIDATION: Verify diff makes sense
	// We added 3 files + 1 new subdir = 4 new leaves minimum
	// We modified 1 file = 1 changed leaf + parent dirs = more changes
	// Parent directories change when children change, so expect multiple added/removed
	t.Logf("  Validating diff logic:")
	t.Logf("    Old DAG: %d leaves", len(senderOldDag.Leafs))
	t.Logf("    New DAG: %d leaves", len(senderNewDag.Leafs))
	t.Logf("    Expected new DAG size = Old + Added - Removed: %d + %d - %d = %d",
		len(senderOldDag.Leafs), d.Summary.Added, d.Summary.Removed,
		len(senderOldDag.Leafs)+d.Summary.Added-d.Summary.Removed)

	expectedSize := len(senderOldDag.Leafs) + d.Summary.Added - d.Summary.Removed
	if expectedSize != len(senderNewDag.Leafs) {
		t.Errorf("❌ Diff math doesn't add up! Expected %d but new DAG has %d leaves",
			expectedSize, len(senderNewDag.Leafs))
	} else {
		t.Logf("  ✓ Diff math checks out: %d = %d", expectedSize, len(senderNewDag.Leafs))
	}

	if d.Summary.Added == 0 {
		t.Fatal("Expected some added leaves in diff")
	}

	// ===================================================================
	// PHASE 5: CREATE PARTIAL DAG FROM DIFF (WITH LINKS, NOT PRUNED)
	// ===================================================================
	t.Log("\nPHASE 5: Creating partial DAG for transmission...")

	partialDag, err := d.CreatePartialDag(senderNewDag)
	if err != nil {
		t.Fatalf("Failed to create partial DAG: %v", err)
	}

	// Verify the partial DAG
	if err := partialDag.Verify(); err != nil {
		t.Fatalf("Partial DAG verification failed: %v", err)
	}

	t.Logf("✓ Partial DAG created: %d leaves (with full link information)", len(partialDag.Leafs))

	// Check that it's marked as partial
	if !partialDag.IsPartial() {
		t.Logf("Note: Partial DAG might include all leaves depending on changes")
	}

	// Debug: Check links in partial
	totalLinks := 0
	for _, leaf := range partialDag.Leafs {
		totalLinks += len(leaf.Links)
	}
	t.Logf("  Partial DAG has %d total links across %d leaves", totalLinks, len(partialDag.Leafs))

	// ===================================================================
	// PHASE 6: TRANSMIT PARTIAL LEAF-BY-LEAF WITH VERIFICATION
	// ===================================================================
	t.Log("\nPHASE 6: Transmitting partial DAG leaf-by-leaf...")

	// Get transmission packets (NOT batched, so proofs are preserved)
	packets := partialDag.GetLeafSequence()
	t.Logf("✓ Generated %d transmission packets", len(packets))

	// ===================================================================
	// PHASE 7: RECEIVER RECONSTRUCTS PARTIAL FROM PACKETS
	// ===================================================================
	t.Log("\nPHASE 7: Receiver reconstructing partial from packets...")

	// Receiver starts with empty DAG
	receiverPartialDag := &dag.Dag{
		Leafs: make(map[string]*dag.DagLeaf),
	}

	// Process each packet
	for i, packet := range packets {
		// Verify packet before applying
		if err := receiverPartialDag.VerifyTransmissionPacket(packet); err != nil {
			t.Fatalf("Packet %d verification failed: %v", i, err)
		}

		// Apply packet
		if err := receiverPartialDag.ApplyAndVerifyTransmissionPacket(packet); err != nil {
			t.Fatalf("Failed to apply packet %d: %v", i, err)
		}

		t.Logf("  ✓ Packet %d/%d applied and verified (%s, %d bytes)",
			i+1, len(packets), packet.Leaf.Type, len(packet.Leaf.Content))
	}

	// Set the root from the partial DAG (we know this from transmission)
	receiverPartialDag.Root = partialDag.Root

	// Debug: Check links in reconstructed partial
	totalReceiverLinks := 0
	for hash, leaf := range receiverPartialDag.Leafs {
		totalReceiverLinks += len(leaf.Links)
		if len(leaf.Links) > 0 {
			t.Logf("  Leaf %s has %d links: %v", hash[:12], len(leaf.Links), leaf.Links)
		}
	}
	t.Logf("  Reconstructed partial has %d total links across %d leaves", totalReceiverLinks, len(receiverPartialDag.Leafs))

	t.Logf("✓ Receiver reconstructed partial: %d leaves", len(receiverPartialDag.Leafs))

	// ===================================================================
	// PHASE 8: VERIFY RECONSTRUCTED PARTIAL DAG
	// ===================================================================
	t.Log("\nPHASE 8: Verifying reconstructed partial DAG...")

	if err := receiverPartialDag.Verify(); err != nil {
		t.Fatalf("Reconstructed partial DAG verification failed: %v", err)
	}

	t.Logf("✓ Reconstructed partial DAG verified successfully")

	// Verify it has the same root as sender's partial
	if receiverPartialDag.Root != partialDag.Root {
		t.Errorf("Root mismatch: sender=%s, receiver=%s",
			partialDag.Root, receiverPartialDag.Root)
	}

	// Verify it has the same number of leaves
	if len(receiverPartialDag.Leafs) != len(partialDag.Leafs) {
		t.Errorf("Leaf count mismatch: sender=%d, receiver=%d",
			len(partialDag.Leafs), len(receiverPartialDag.Leafs))
	}

	// ===================================================================
	// PHASE 9: RECEIVER CREATES DIFF USING OLD DAG + RECEIVED PARTIAL
	// ===================================================================
	t.Log("\nPHASE 9: Receiver creating diff from received partial...")

	// Receiver has the same old DAG as sender (they're synchronized)
	receiverOldDag := senderOldDag

	// Create diff from received leaves
	receiverDiff, err := diff.DiffFromNewLeaves(receiverOldDag, receiverPartialDag.Leafs)
	if err != nil {
		t.Fatalf("Failed to create receiver diff: %v", err)
	}

	t.Logf("✓ Receiver diff created: %d added, %d removed",
		receiverDiff.Summary.Added, receiverDiff.Summary.Removed)

	// VALIDATION: The receiver diff should match the sender diff!
	t.Logf("  Comparing receiver diff with sender diff:")
	t.Logf("    Sender:   %d added, %d removed", d.Summary.Added, d.Summary.Removed)
	t.Logf("    Receiver: %d added, %d removed", receiverDiff.Summary.Added, receiverDiff.Summary.Removed)

	if receiverDiff.Summary.Added != d.Summary.Added {
		t.Errorf("❌ Added leaves mismatch! Sender=%d, Receiver=%d",
			d.Summary.Added, receiverDiff.Summary.Added)
	}

	if receiverDiff.Summary.Removed != d.Summary.Removed {
		t.Errorf("❌ Removed leaves mismatch! Sender=%d, Receiver=%d",
			d.Summary.Removed, receiverDiff.Summary.Removed)
	}

	// Check if the actual leaf hashes match
	senderAdded := d.GetAddedLeaves()
	receiverAdded := receiverDiff.GetAddedLeaves()

	if len(senderAdded) != len(receiverAdded) {
		t.Errorf("❌ Added leaf count mismatch! Sender=%d, Receiver=%d",
			len(senderAdded), len(receiverAdded))
	}

	for bareHash := range senderAdded {
		if _, exists := receiverAdded[bareHash]; !exists {
			t.Errorf("❌ Receiver missing added leaf: %s", bareHash[:12])
		}
	}

	// ===================================================================
	// PHASE 10: RECEIVER APPLIES DIFF TO CREATE NEW DAG
	// ===================================================================
	t.Log("\nPHASE 10: Receiver applying diff to create new DAG...")

	// Debug: Check what's in the diff
	t.Logf("  Diff has %d added leaves, %d removed leaves", receiverDiff.Summary.Added, receiverDiff.Summary.Removed)
	t.Logf("  Old DAG has %d leaves", len(receiverOldDag.Leafs))

	receiverNewDag, err := receiverDiff.ApplyToDAG(receiverOldDag)
	if err != nil {
		t.Fatalf("Failed to apply diff on receiver: %v", err)
	}

	t.Logf("✓ Receiver new DAG created: %d leaves", len(receiverNewDag.Leafs))

	// Verify receiver's new DAG
	if err := receiverNewDag.Verify(); err != nil {
		t.Fatalf("Receiver new DAG verification failed: %v", err)
	}

	t.Logf("✓ Receiver new DAG verified successfully")

	// ===================================================================
	// PHASE 11: VERIFY FINAL DAG MATCHES SENDER'S NEW DAG
	// ===================================================================
	t.Log("\nPHASE 11: Verifying receiver's DAG matches sender's new DAG...")

	// Compare leaf counts
	if len(receiverNewDag.Leafs) != len(senderNewDag.Leafs) {
		t.Errorf("Leaf count mismatch: sender=%d, receiver=%d",
			len(senderNewDag.Leafs), len(receiverNewDag.Leafs))
	}

	// Compare roots
	if receiverNewDag.Root != senderNewDag.Root {
		t.Errorf("Root mismatch: sender=%s, receiver=%s",
			senderNewDag.Root, receiverNewDag.Root)
	}

	// Compare all leaves (by bare hash)
	senderBareHashes := make(map[string]*dag.DagLeaf)
	for hash, leaf := range senderNewDag.Leafs {
		senderBareHashes[hash] = leaf
	}

	receiverBareHashes := make(map[string]*dag.DagLeaf)
	for hash, leaf := range receiverNewDag.Leafs {
		receiverBareHashes[hash] = leaf
	}

	// Verify all sender leaves exist in receiver
	for bareHash, senderLeaf := range senderBareHashes {
		receiverLeaf, exists := receiverBareHashes[bareHash]
		if !exists {
			t.Errorf("Missing leaf in receiver: %s (name: %s)", bareHash[:16], senderLeaf.ItemName)
			continue
		}

		// Verify leaf properties match
		if receiverLeaf.Type != senderLeaf.Type {
			t.Errorf("Leaf %s type mismatch: sender=%s, receiver=%s",
				bareHash[:16], senderLeaf.Type, receiverLeaf.Type)
		}
		if receiverLeaf.ItemName != senderLeaf.ItemName {
			t.Errorf("Leaf %s name mismatch: sender=%s, receiver=%s",
				bareHash[:16], senderLeaf.ItemName, receiverLeaf.ItemName)
		}
		if receiverLeaf.ContentSize != senderLeaf.ContentSize {
			t.Errorf("Leaf %s size mismatch: sender=%d, receiver=%d",
				bareHash[:16], senderLeaf.ContentSize, receiverLeaf.ContentSize)
		}
	}

	// Verify no extra leaves in receiver
	for bareHash := range receiverBareHashes {
		if _, exists := senderBareHashes[bareHash]; !exists {
			t.Errorf("Extra leaf in receiver: %s", bareHash[:16])
		}
	}

	t.Logf("✓ All leaves match between sender and receiver")

	// ===================================================================
	// SUCCESS!
	// ===================================================================
	t.Log("\n" + strings.Repeat("=", 70))
	t.Log("SUCCESS! Complete workflow validated:")
	t.Log("  ✓ Created deep hierarchy with multiple levels")
	t.Log("  ✓ Modified files and directories")
	t.Log("  ✓ Created diff between old and new states")
	t.Log("  ✓ Transmitted partial DAG leaf-by-leaf")
	t.Log("  ✓ Verified each packet during transmission")
	t.Log("  ✓ Reconstructed partial on receiver side")
	t.Log("  ✓ Created new DAG from diff on receiver side")
	t.Log("  ✓ Verified all DAGs at each step")
	t.Log("  ✓ Final DAG matches original exactly")
	t.Log(strings.Repeat("=", 70))
}

// createDeepHierarchy creates a nested directory structure for testing
// depth: how many levels deep
// filesPerDir: how many files to create in each directory
func createDeepHierarchy(basePath string, depth int, filesPerDir int) error {
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return err
	}

	var createLevel func(path string, currentDepth int) error
	createLevel = func(path string, currentDepth int) error {
		// Create files at this level
		for i := 0; i < filesPerDir; i++ {
			filename := filepath.Join(path, "file_"+string(rune('0'+i))+".txt")
			content := []byte("Content at depth " + string(rune('0'+currentDepth)) + " file " + string(rune('0'+i)))
			if err := os.WriteFile(filename, content, 0644); err != nil {
				return err
			}
		}

		// Create subdirectories if not at max depth
		if currentDepth < depth {
			for i := 0; i < 2; i++ { // Create 2 subdirectories at each level
				subdir := filepath.Join(path, "level"+string(rune('0'+currentDepth))+"_"+string(rune('0'+i)))
				if err := os.MkdirAll(subdir, 0755); err != nil {
					return err
				}
				if err := createLevel(subdir, currentDepth+1); err != nil {
					return err
				}
			}
		}

		return nil
	}

	return createLevel(basePath, 1)
}
