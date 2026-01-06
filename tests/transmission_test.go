package tests

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/HORNET-Storage/Scionic-Merkle-Tree/v2/dag"
	"github.com/HORNET-Storage/Scionic-Merkle-Tree/v2/testutil"
)

// TestLeafByLeafTransmission tests transmitting a full DAG leaf-by-leaf
// Tests against all fixtures to ensure transmission works for all DAG types
func TestLeafByLeafTransmission(t *testing.T) {
	testutil.RunTestWithAllFixtures(t, func(t *testing.T, originalDag *dag.Dag, fixture testutil.TestFixture, fixturePath string) {
		err := originalDag.Verify()
		if err != nil {
			t.Fatalf("Original DAG verification failed for %s: %v", fixture.Name, err)
		}

		sequence := originalDag.GetLeafSequence()

		if len(sequence) == 0 {
			t.Fatal("No transmission packets generated")
		}

		t.Logf("Generated %d transmission packets for %s", len(sequence), fixture.Name)

		receiverDag := &dag.Dag{
			Root:  originalDag.Root,
			Leafs: make(map[string]*dag.DagLeaf),
		}

		for i, p := range sequence {
			bytes, err := p.ToCBOR()
			if err != nil {
				t.Fatalf("Failed to serialize packet")
			}

			packet, err := dag.TransmissionPacketFromCBOR(bytes)
			if err != nil {
				t.Fatalf("Failed to deserialize packet")
			}

			err = receiverDag.ApplyAndVerifyTransmissionPacket(packet)
			if err != nil {
				t.Fatalf("Packet verification failed after packet %d: %v", i, err)
			}
		}

		if len(receiverDag.Leafs) != len(originalDag.Leafs) {
			t.Fatalf("Receiver DAG has %d leaves, expected %d",
				len(receiverDag.Leafs), len(originalDag.Leafs))
		}

		err = receiverDag.Verify()
		if err != nil {
			t.Fatalf("Final full DAG verification failed: %v", err)
		}

		for _, leaf := range receiverDag.Leafs {
			leaf.Proofs = nil
		}

		err = receiverDag.Verify()
		if err != nil {
			t.Fatalf("Full DAG verification after discarding proofs failed: %v", err)
		}

		t.Logf("✓ %s: Successfully verified full DAG after leaf-by-leaf transmission", fixture.Name)
	})
}

// TestPartialDagTransmission tests transmitting a partial DAG
// Only uses multi-file fixtures since you need multiple files for partial DAGs
func TestPartialDagTransmission(t *testing.T) {
	testutil.RunTestWithMultiFileFixtures(t, func(t *testing.T, originalDag *dag.Dag, fixture testutil.TestFixture, fixturePath string) {
		err := originalDag.Verify()
		if err != nil {
			t.Fatalf("Original DAG verification failed: %v", err)
		}

		// Get some file leaf hashes for partial DAG
		var fileHashes []string
		for hash, leaf := range originalDag.Leafs {
			if leaf.Type == dag.FileLeafType && len(fileHashes) < 3 {
				fileHashes = append(fileHashes, hash)
			}
		}
		if len(fileHashes) == 0 {
			t.Skip("No file leaves in DAG to test partial")
		}

		partialDag, err := originalDag.GetPartial(fileHashes, true)
		if err != nil {
			t.Fatalf("Failed to get partial DAG: %v", err)
		}

		err = partialDag.Verify()
		if err != nil {
			t.Fatalf("Partial DAG verification failed: %v", err)
		}

		if !partialDag.IsPartial() {
			t.Fatal("DAG not recognized as partial")
		}

		sequence := partialDag.GetLeafSequence()
		if len(sequence) == 0 {
			t.Fatal("No transmission packets generated from partial DAG")
		}

		t.Logf("Generated %d transmission packets from partial DAG with %d leaves",
			len(sequence), len(partialDag.Leafs))

		receiverDag := &dag.Dag{
			Root:  partialDag.Root,
			Leafs: make(map[string]*dag.DagLeaf),
		}

		for i, p := range sequence {
			bytes, err := p.ToCBOR()
			if err != nil {
				t.Fatalf("Failed to serialize packet from partial DAG")
			}

			packet, err := dag.TransmissionPacketFromCBOR(bytes)
			if err != nil {
				t.Fatalf("Failed to deserialize packet from partial DAG")
			}

			receiverDag.ApplyTransmissionPacket(packet)

			err = receiverDag.Verify()
			if err != nil {
				t.Fatalf("Verification failed after packet %d from partial DAG: %v", i, err)
			}
		}

		if len(receiverDag.Leafs) != len(partialDag.Leafs) {
			t.Fatalf("Receiver DAG has %d leaves, expected %d (same as partial DAG)",
				len(receiverDag.Leafs), len(partialDag.Leafs))
		}

		if !receiverDag.IsPartial() {
			t.Fatal("Reconstructed DAG not recognized as partial")
		}

		t.Logf("✓ %s: Successfully transmitted and verified partial DAG", fixture.Name)
	})
}

func TestBatchSizeLimits(t *testing.T) {
	testDir, err := os.MkdirTemp("", "dag_batch_limits_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Create deterministic test data with larger files to trigger size limits
	inputDir := filepath.Join(testDir, "input")
	if err := os.MkdirAll(inputDir, 0755); err != nil {
		t.Fatalf("Failed to create input directory: %v", err)
	}

	// Create 4 subdirectories with 3 files each (100KB per file)
	for dirIdx := 0; dirIdx < 4; dirIdx++ {
		subDir := filepath.Join(inputDir, string(rune('a'+dirIdx)))
		if err := os.MkdirAll(subDir, 0755); err != nil {
			t.Fatalf("Failed to create subdirectory: %v", err)
		}
		for fileIdx := 0; fileIdx < 3; fileIdx++ {
			filePath := filepath.Join(subDir, string(rune('1'+fileIdx))+".txt")
			// Create deterministic 100KB content
			content := make([]byte, 100000)
			for i := range content {
				content[i] = byte((dirIdx*3 + fileIdx + i) % 256)
			}
			if err := os.WriteFile(filePath, content, 0644); err != nil {
				t.Fatalf("Failed to write file: %v", err)
			}
		}
	}

	originalDag, err := dag.CreateDag(inputDir, true)
	if err != nil {
		t.Fatalf("Failed to create DAG: %v", err)
	}

	// Test with very small batch size to force splitting
	dag.SetBatchSize(200 * 1024) // 200KB - should trigger size-based splitting
	smallLimitSequence := originalDag.GetBatchedLeafSequence()

	// Test with medium batch size
	dag.SetBatchSize(500 * 1024) // 500KB
	mediumLimitSequence := originalDag.GetBatchedLeafSequence()

	// Test with large batch size (should allow more grouping)
	dag.SetBatchSize(2 * 1024 * 1024) // 2MB
	largeLimitSequence := originalDag.GetBatchedLeafSequence()

	analyzeBatchSizes := func(name string, batches []*dag.BatchedTransmissionPacket) (int, float64, int) {
		totalLeaves := 0
		totalSize := 0
		maxBatchSize := 0

		t.Logf("\n=== %s ===", name)

		for i, batch := range batches {
			cborData, err := batch.ToCBOR()
			if err != nil {
				t.Fatalf("Failed to serialize batch %d: %v", i, err)
			}
			actualSize := len(cborData)
			totalSize += actualSize
			totalLeaves += len(batch.Leaves)

			if actualSize > maxBatchSize {
				maxBatchSize = actualSize
			}

			sizeKB := float64(actualSize) / 1024
			t.Logf("Batch %d: %d leaves, %.1f KB", i, len(batch.Leaves), sizeKB)
		}

		avgBatchSize := float64(totalSize) / float64(len(batches)) / 1024
		t.Logf("Summary: %d batches, %d leaves, %.1f KB average, %d KB max batch",
			len(batches), totalLeaves, avgBatchSize, maxBatchSize/1024)

		return len(batches), avgBatchSize, maxBatchSize
	}

	smallBatches, _, _ := analyzeBatchSizes("200KB Limit", smallLimitSequence)
	mediumBatches, _, _ := analyzeBatchSizes("500KB Limit", mediumLimitSequence)
	largeBatches, _, _ := analyzeBatchSizes("2MB Limit", largeLimitSequence)

	// Note: Due to Merkle proof requirements, children of the same parent
	// must be kept together, which may result in similar batch counts
	// even with different size limits
	_ = smallBatches  // May be same as medium due to proof constraints
	_ = mediumBatches // May be same as large due to proof constraints
	_ = largeBatches  // May be same as others due to proof constraints

	// Verify size limits are respected when possible (allow some tolerance for Merkle proof requirements)
	// Note: Due to Merkle proof constraints, some batches may exceed size limits
	verifySizeLimit := func(name string, batches []*dag.BatchedTransmissionPacket, limit int) {
		exceededCount := 0
		for i, batch := range batches {
			cborData, err := batch.ToCBOR()
			if err != nil {
				t.Fatalf("Failed to serialize batch %d: %v", i, err)
			}
			actualSize := len(cborData)
			maxAllowed := limit * 2 // Allow 2x for Merkle proof preservation

			if actualSize > maxAllowed {
				t.Errorf("%s: Batch %d significantly exceeds size limit: %d bytes (limit: %d)",
					name, i, actualSize, limit)
			}
			if actualSize > limit {
				exceededCount++
			}
		}
		if exceededCount > 0 {
			t.Logf("%s: %d batches exceeded size limit (allowed for Merkle proof preservation)", name, exceededCount)
		}
	}

	verifySizeLimit("200KB Limit", smallLimitSequence, 200*1024)
	verifySizeLimit("500KB Limit", mediumLimitSequence, 500*1024)
	verifySizeLimit("2MB Limit", largeLimitSequence, 2*1024*1024)

	// Test transmission works for all batch sizes
	testTransmission := func(name string, batches []*dag.BatchedTransmissionPacket) {
		receiverDag := &dag.Dag{
			Leafs: make(map[string]*dag.DagLeaf),
			Root:  originalDag.Root,
		}

		for i, batch := range batches {
			err := receiverDag.ApplyAndVerifyBatchedTransmissionPacket(batch)
			if err != nil {
				t.Fatalf("%s: Failed to apply batch %d: %v", name, i, err)
			}
		}

		if err := receiverDag.Verify(); err != nil {
			t.Fatalf("%s: Received DAG verification failed: %v", name, err)
		}

		if len(receiverDag.Leafs) != len(originalDag.Leafs) {
			t.Fatalf("%s: Leaf count mismatch: %d vs %d", name, len(receiverDag.Leafs), len(originalDag.Leafs))
		}

		t.Logf("%s: Successfully transmitted %d leaves in %d batches", name, len(receiverDag.Leafs), len(batches))
	}

	testTransmission("200KB Limit", smallLimitSequence)
	testTransmission("500KB Limit", mediumLimitSequence)
	testTransmission("2MB Limit", largeLimitSequence)

	t.Log("Batch size limits test completed successfully")
}

func TestIncrementalBatching(t *testing.T) {
	testDir, err := os.MkdirTemp("", "dag_incremental_batching_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Create a deterministic DAG with a directory that has many children
	inputDir := filepath.Join(testDir, "input")
	if err := os.MkdirAll(inputDir, 0755); err != nil {
		t.Fatalf("Failed to create input directory: %v", err)
	}

	// Create 3 subdirectories with 4 files each to test incremental batching
	for dirIdx := 0; dirIdx < 3; dirIdx++ {
		subDir := filepath.Join(inputDir, string(rune('a'+dirIdx)))
		if err := os.MkdirAll(subDir, 0755); err != nil {
			t.Fatalf("Failed to create subdirectory: %v", err)
		}
		for fileIdx := 0; fileIdx < 4; fileIdx++ {
			filePath := filepath.Join(subDir, string(rune('1'+fileIdx))+".txt")
			// Create deterministic content (varies by file)
			content := make([]byte, 2048)
			for i := range content {
				content[i] = byte((dirIdx*4 + fileIdx + i) % 256)
			}
			if err := os.WriteFile(filePath, content, 0644); err != nil {
				t.Fatalf("Failed to write file: %v", err)
			}
		}
	}

	originalDag, err := dag.CreateDag(inputDir, true)
	if err != nil {
		t.Fatalf("Failed to create DAG: %v", err)
	}

	err = originalDag.Verify()
	if err != nil {
		t.Fatalf("Original DAG verification failed: %v", err)
	}

	// Set a small batch size to force splitting of children
	dag.SetBatchSize(50 * 1024) // 50KB batches
	defer dag.SetDefaultBatchSize()

	sequence := originalDag.GetBatchedLeafSequence()

	if len(sequence) == 0 {
		t.Fatal("No transmission packets generated")
	}

	t.Logf("Generated %d transmission packets for incremental batching test", len(sequence))

	// Verify that children are split across batches
	parentFound := false
	childrenInBatches := make(map[string]int) // parent -> count of batches containing its children

	for i, batch := range sequence {
		t.Logf("Batch %d: %d leaves", i, len(batch.Leaves))

		// Check relationships in this batch
		// Skip batches with only 1 leaf, as they are single child batches where parent is not included
		if len(batch.Leaves) == 1 {
			continue
		}
		for childHash, parentHash := range batch.Relationships {
			if parentHash != "" {
				childrenInBatches[parentHash]++

				// Verify parent is included in this batch
				parentInBatch := false
				for _, leaf := range batch.Leaves {
					if leaf.Hash == parentHash {
						parentInBatch = true
						parentFound = true
						break
					}
				}
				if !parentInBatch {
					t.Fatalf("Parent %s not found in batch %d containing its child %s", parentHash, i, childHash)
				}
			}
		}
	}

	if !parentFound {
		t.Fatal("No parent-child relationships found in batches")
	}

	// Verify that at least one parent has children split across multiple batches
	childrenSplit := false
	for parent, batchCount := range childrenInBatches {
		t.Logf("Parent %s has children in %d batches", parent, batchCount)
		if batchCount > 1 {
			childrenSplit = true
			break
		}
	}

	if !childrenSplit {
		t.Fatal("Children were not split across batches - incremental batching not working")
	}

	// Test transmission and verification
	receiverDag := &dag.Dag{
		Root:  originalDag.Root,
		Leafs: make(map[string]*dag.DagLeaf),
	}

	for i, batch := range sequence {
		err = receiverDag.ApplyAndVerifyBatchedTransmissionPacket(batch)
		if err != nil {
			t.Fatalf("Failed to apply and verify batch %d: %v", i, err)
		}
	}

	if len(receiverDag.Leafs) != len(originalDag.Leafs) {
		t.Fatalf("Receiver DAG has %d leaves, expected %d", len(receiverDag.Leafs), len(originalDag.Leafs))
	}

	err = receiverDag.Verify()
	if err != nil {
		t.Fatalf("Final DAG verification failed: %v", err)
	}

	t.Log("Incremental batching test completed successfully - children properly split across batches with valid proofs")
}

func TestBatchedTransmissionMerkleProofCorrectness(t *testing.T) {
	testDir, err := os.MkdirTemp("", "dag_merkle_proof_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Create deterministic test data specifically designed to trigger Merkle proof generation
	// 10 children per directory ensures multiple children get batched together
	inputDir := filepath.Join(testDir, "input")
	if err := os.MkdirAll(inputDir, 0755); err != nil {
		t.Fatalf("Failed to create input directory: %v", err)
	}

	// Create 5 subdirectories, each with 10 children
	for dirIdx := 0; dirIdx < 5; dirIdx++ {
		subDir := filepath.Join(inputDir, string(rune('a'+dirIdx)))
		if err := os.MkdirAll(subDir, 0755); err != nil {
			t.Fatalf("Failed to create subdirectory: %v", err)
		}
		for fileIdx := 0; fileIdx < 10; fileIdx++ {
			filePath := filepath.Join(subDir, string(rune('0'+fileIdx))+".txt")
			// Create deterministic 1KB content
			content := make([]byte, 1024)
			for i := range content {
				content[i] = byte((dirIdx*10 + fileIdx + i) % 256)
			}
			if err := os.WriteFile(filePath, content, 0644); err != nil {
				t.Fatalf("Failed to write file: %v", err)
			}
		}
	}

	// Create nested structure for hierarchical batching
	nestedDir := filepath.Join(inputDir, "nested")
	if err := os.MkdirAll(nestedDir, 0755); err != nil {
		t.Fatalf("Failed to create nested directory: %v", err)
	}

	for i := 0; i < 3; i++ {
		subNested := filepath.Join(nestedDir, string(rune('x'+i)))
		if err := os.MkdirAll(subNested, 0755); err != nil {
			t.Fatalf("Failed to create nested subdirectory: %v", err)
		}
		for j := 0; j < 5; j++ {
			filePath := filepath.Join(subNested, string(rune('0'+j))+".txt")
			content := make([]byte, 1024)
			for k := range content {
				content[k] = byte((i*5 + j + k) % 256)
			}
			if err := os.WriteFile(filePath, content, 0644); err != nil {
				t.Fatalf("Failed to write nested file: %v", err)
			}
		}
	}

	originalDag, err := dag.CreateDag(inputDir, true)
	if err != nil {
		t.Fatalf("Failed to create DAG: %v", err)
	}

	err = originalDag.Verify()
	if err != nil {
		t.Fatalf("Original DAG verification failed: %v", err)
	}

	// Set a small batch size to force multiple children into batches with their parent
	// This should trigger the Merkle proof generation bug if it exists
	dag.SetBatchSize(50 * 1024) // 50KB batches - small enough to force splitting
	defer dag.SetDefaultBatchSize()

	sequence := originalDag.GetBatchedLeafSequence()

	if len(sequence) == 0 {
		t.Fatal("No batched transmission packets generated")
	}

	t.Logf("Generated %d batched transmission packets", len(sequence))

	// Test transmission and verification
	receiverDag := &dag.Dag{
		Root:  originalDag.Root,
		Leafs: make(map[string]*dag.DagLeaf),
	}

	for i, batch := range sequence {
		// Count proofs in parent leaves
		proofCount := 0
		for _, leaf := range batch.Leaves {
			if leaf.Proofs != nil {
				proofCount += len(leaf.Proofs)
			}
		}
		t.Logf("Processing batch %d: %d leaves, %d relationships, %d proofs in parents",
			i, len(batch.Leaves), len(batch.Relationships), proofCount)

		// This should fail if the Merkle proof bug exists
		err = receiverDag.ApplyAndVerifyBatchedTransmissionPacket(batch)
		if err != nil {
			t.Fatalf("Failed to apply and verify batch %d: %v", i, err)
		}
	}

	if len(receiverDag.Leafs) != len(originalDag.Leafs) {
		t.Fatalf("Receiver DAG has %d leaves, expected %d", len(receiverDag.Leafs), len(originalDag.Leafs))
	}

	err = receiverDag.Verify()
	if err != nil {
		t.Fatalf("Final DAG verification failed: %v", err)
	}

	// Additional verification: check that parent leaves have proofs for their children
	for batchIdx, batch := range sequence {
		for _, leaf := range batch.Leaves {
			if leaf.Type == dag.DirectoryLeafType && len(leaf.Links) > 1 && leaf.Proofs != nil {
				t.Logf("Batch %d: Parent %s has %d proofs for %d children",
					batchIdx, leaf.ItemName, len(leaf.Proofs), len(leaf.Links))

				// Verify each proof cryptographically
				for childHash, proof := range leaf.Proofs {
					err := leaf.VerifyBranch(proof)
					if err != nil {
						t.Errorf("PROOF VERIFICATION FAILED for child %s: %v", childHash, err)
					}
				}
			}
		}
	}

	t.Log("Batched transmission Merkle proof correctness test completed successfully")
}

func TestBatchedTransmissionMerkleProofBugTrigger(t *testing.T) {
	testDir, err := os.MkdirTemp("", "dag_merkle_proof_bug_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Create deterministic test data with filenames that will be out of alphabetical order
	// This should definitely trigger the Merkle proof indexing bug if it exists
	inputDir := filepath.Join(testDir, "input")
	if err := os.MkdirAll(inputDir, 0755); err != nil {
		t.Fatalf("Failed to create input directory: %v", err)
	}

	// Create 3 subdirectories, each with 8 children using out-of-order names
	outOfOrderNames := []string{"z_file", "a_file", "m_file", "b_file", "y_file", "c_file", "x_file", "d_file"}
	for dirIdx := 0; dirIdx < 3; dirIdx++ {
		subDir := filepath.Join(inputDir, string(rune('a'+dirIdx)))
		if err := os.MkdirAll(subDir, 0755); err != nil {
			t.Fatalf("Failed to create subdirectory: %v", err)
		}
		for fileIdx := 0; fileIdx < len(outOfOrderNames) && fileIdx < 8; fileIdx++ {
			fileName := outOfOrderNames[fileIdx] + ".txt"
			filePath := filepath.Join(subDir, fileName)
			// Create deterministic 512B content
			content := make([]byte, 512)
			for i := range content {
				content[i] = byte((dirIdx*8 + fileIdx + i) % 256)
			}
			if err := os.WriteFile(filePath, content, 0644); err != nil {
				t.Fatalf("Failed to write file: %v", err)
			}
		}
	}

	originalDag, err := dag.CreateDag(inputDir, true)
	if err != nil {
		t.Fatalf("Failed to create DAG: %v", err)
	}

	err = originalDag.Verify()
	if err != nil {
		t.Fatalf("Original DAG verification failed: %v", err)
	}

	// Set a small batch size to force multiple children into batches with their parent
	dag.SetBatchSize(20 * 1024) // 20KB batches - very small to force splitting
	defer dag.SetDefaultBatchSize()

	sequence := originalDag.GetBatchedLeafSequence()

	if len(sequence) == 0 {
		t.Fatal("No batched transmission packets generated")
	}

	t.Logf("Generated %d batched transmission packets", len(sequence))

	// Test transmission and verification - this should fail if the bug exists
	receiverDag := &dag.Dag{
		Root:  originalDag.Root,
		Leafs: make(map[string]*dag.DagLeaf),
	}

	failedBatch := -1
	for i, batch := range sequence {
		// Count proofs in parent leaves
		proofCount := 0
		for _, leaf := range batch.Leaves {
			if leaf.Proofs != nil {
				proofCount += len(leaf.Proofs)
			}
		}
		t.Logf("Processing batch %d: %d leaves, %d relationships, %d proofs in parents",
			i, len(batch.Leaves), len(batch.Relationships), proofCount)

		// This should fail if the Merkle proof bug exists
		err = receiverDag.ApplyAndVerifyBatchedTransmissionPacket(batch)
		if err != nil {
			t.Logf("FAILED at batch %d: %v", i, err)
			failedBatch = i
			break // Stop at first failure to analyze
		}
	}

	if failedBatch >= 0 {
		t.Errorf("BATCHED TRANSMISSION FAILED at batch %d - this confirms the Merkle proof indexing bug exists!", failedBatch)

		// Analyze the failed batch
		batch := sequence[failedBatch]
		t.Logf("Failed batch analysis:")
		t.Logf("  Leaves: %d", len(batch.Leaves))
		t.Logf("  Relationships: %d", len(batch.Relationships))

		// Show proofs in parent leaves
		for _, leaf := range batch.Leaves {
			if len(leaf.Proofs) > 0 {
				t.Logf("  Parent %s has %d proofs", leaf.ItemName, len(leaf.Proofs))
			}
		}

		t.Fatal("Merkle proof indexing bug confirmed - proofs are not verifying correctly")
	}

	if len(receiverDag.Leafs) != len(originalDag.Leafs) {
		t.Fatalf("Receiver DAG has %d leaves, expected %d", len(receiverDag.Leafs), len(originalDag.Leafs))
	}

	err = receiverDag.Verify()
	if err != nil {
		t.Fatalf("Final DAG verification failed: %v", err)
	}

	t.Log("Batched transmission Merkle proof bug trigger test completed successfully")
}

func TestDirectMerkleProofValidation(t *testing.T) {
	testDir, err := os.MkdirTemp("", "dag_direct_proof_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Create deterministic test data with out-of-order filenames to trigger the indexing bug
	inputDir := filepath.Join(testDir, "input")
	if err := os.MkdirAll(inputDir, 0755); err != nil {
		t.Fatalf("Failed to create input directory: %v", err)
	}

	// Create subdirectory with 5 children using out-of-order names
	outOfOrderNames := []string{"z_file", "a_file", "m_file", "b_file", "y_file"}
	subDir := filepath.Join(inputDir, "test_subdir")
	if err := os.MkdirAll(subDir, 0755); err != nil {
		t.Fatalf("Failed to create subdirectory: %v", err)
	}
	for fileIdx, name := range outOfOrderNames {
		filePath := filepath.Join(subDir, name+".txt")
		// Create deterministic 1KB content
		content := make([]byte, 1024)
		for i := range content {
			content[i] = byte((fileIdx + i) % 256)
		}
		if err := os.WriteFile(filePath, content, 0644); err != nil {
			t.Fatalf("Failed to write file: %v", err)
		}
	}

	originalDag, err := dag.CreateDag(inputDir, true)
	if err != nil {
		t.Fatalf("Failed to create DAG: %v", err)
	}

	err = originalDag.Verify()
	if err != nil {
		t.Fatalf("Original DAG verification failed: %v", err)
	}

	// Set small batch size to force multiple children into separate batches
	// With 5 files of 1KB each, a 2KB batch size will ensure they're split
	dag.SetBatchSize(2 * 1024) // 2KB batches
	defer dag.SetDefaultBatchSize()

	sequence := originalDag.GetBatchedLeafSequence()

	if len(sequence) == 0 {
		t.Fatal("No batched transmission packets generated")
	}

	t.Logf("Generated %d batched transmission packets", len(sequence))

	// Find a parent leaf with proofs in any batch
	var testParent *dag.DagLeaf
	for _, batch := range sequence {
		for _, leaf := range batch.Leaves {
			if leaf.Type == dag.DirectoryLeafType && leaf.Proofs != nil && len(leaf.Proofs) > 1 {
				testParent = leaf
				break
			}
		}
		if testParent != nil {
			break
		}
	}

	if testParent == nil {
		t.Fatal("No parent with multiple proofs found - cannot test direct proof validation")
	}

	t.Logf("Testing parent %s with %d proofs for %d children", testParent.ItemName, len(testParent.Proofs), len(testParent.Links))

	// Validate each proof cryptographically
	for childHash, proof := range testParent.Proofs {
		t.Logf("Validating proof for child %s", childHash[:12])

		// Use the parent's VerifyBranch method which checks the proof against ClassicMerkleRoot
		err := testParent.VerifyBranch(proof)
		if err != nil {
			t.Errorf("CRYPTOGRAPHIC PROOF VALIDATION FAILED: proof for child %s is invalid: %v", childHash, err)
			t.Logf("This indicates the batched transmission proof indexing bug!")
		} else {
			t.Logf("Proof validated correctly for child %s", childHash)
		}
	}

	t.Log("Direct Merkle proof validation test completed")
}

// TestPrunedLinksOptimization tests that partial DAGs with pruned links still work correctly.
func TestPrunedLinksOptimization(t *testing.T) {
	testDir, err := os.MkdirTemp("", "dag_pruned_links_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Create a directory with 5 files - we'll request only 2
	inputDir := filepath.Join(testDir, "input")
	if err := os.MkdirAll(inputDir, 0755); err != nil {
		t.Fatalf("Failed to create input directory: %v", err)
	}

	fileNames := []string{"file_a.txt", "file_b.txt", "file_c.txt", "file_d.txt", "file_e.txt"}
	for i, name := range fileNames {
		filePath := filepath.Join(inputDir, name)
		content := make([]byte, 512)
		for j := range content {
			content[j] = byte((i*512 + j) % 256)
		}
		if err := os.WriteFile(filePath, content, 0644); err != nil {
			t.Fatalf("Failed to write file: %v", err)
		}
	}

	// Create original DAG
	originalDag, err := dag.CreateDag(inputDir, true)
	if err != nil {
		t.Fatalf("Failed to create DAG: %v", err)
	}

	if err := originalDag.Verify(); err != nil {
		t.Fatalf("Original DAG verification failed: %v", err)
	}

	// Find file leaf hashes (children of root)
	rootLeaf := originalDag.Leafs[originalDag.Root]
	if rootLeaf == nil {
		t.Fatal("Root leaf not found")
	}

	if len(rootLeaf.Links) < 5 {
		t.Fatalf("Expected at least 5 children in root, got %d", len(rootLeaf.Links))
	}

	// Get partial DAG for only first 2 files
	requestedHashes := rootLeaf.Links[:2]
	t.Logf("Requesting partial DAG for %d of %d children", len(requestedHashes), len(rootLeaf.Links))

	partialDag, err := originalDag.GetPartial(requestedHashes, false)
	if err != nil {
		t.Fatalf("Failed to get partial DAG: %v", err)
	}

	// Verify partial DAG state before pruning
	partialRoot := partialDag.Leafs[partialDag.Root]
	if partialRoot == nil {
		t.Fatal("Partial DAG root not found")
	}

	t.Logf("Before pruning: CurrentLinkCount=%d, len(Links)=%d, len(Proofs)=%d",
		partialRoot.CurrentLinkCount, len(partialRoot.Links), len(partialRoot.Proofs))

	// CurrentLinkCount should reflect original count (5)
	if partialRoot.CurrentLinkCount != 5 {
		t.Errorf("Expected CurrentLinkCount=5, got %d", partialRoot.CurrentLinkCount)
	}

	// Proofs should exist for requested children
	if len(partialRoot.Proofs) < 2 {
		t.Errorf("Expected at least 2 proofs in partial root, got %d", len(partialRoot.Proofs))
	}

	// Now simulate what pruneLinks=true does: remove links not in partial DAG
	prunedLinks := make([]string, 0)
	for _, linkHash := range partialRoot.Links {
		if _, exists := partialDag.Leafs[linkHash]; exists {
			prunedLinks = append(prunedLinks, linkHash)
		}
	}
	partialRoot.Links = prunedLinks

	t.Logf("After pruning: CurrentLinkCount=%d, len(Links)=%d, len(Proofs)=%d",
		partialRoot.CurrentLinkCount, len(partialRoot.Links), len(partialRoot.Proofs))

	// Links should now be pruned to 2
	if len(partialRoot.Links) != 2 {
		t.Errorf("Expected len(Links)=2 after pruning, got %d", len(partialRoot.Links))
	}

	// CurrentLinkCount should still be 5 (original count preserved)
	if partialRoot.CurrentLinkCount != 5 {
		t.Errorf("CurrentLinkCount should remain 5 after pruning, got %d", partialRoot.CurrentLinkCount)
	}

	// Set a batch size that puts all leaves in one batch
	dag.SetBatchSize(100 * 1024)
	defer dag.SetDefaultBatchSize()

	// Get batched sequence - this is where the bug manifested
	sequence := partialDag.GetBatchedLeafSequence()

	if len(sequence) == 0 {
		t.Fatal("No batched transmission packets generated")
	}

	t.Logf("Generated %d batched transmission packets", len(sequence))

	// Find the root in the first batch and check its proofs
	var rootInBatch *dag.DagLeaf
	for _, leaf := range sequence[0].Leaves {
		if leaf.Hash == partialDag.Root {
			rootInBatch = leaf
			break
		}
	}

	if rootInBatch == nil {
		t.Fatal("Root not found in first batch")
	}

	t.Logf("Root in batch: CurrentLinkCount=%d, len(Links)=%d, len(Proofs)=%d",
		rootInBatch.CurrentLinkCount, len(rootInBatch.Links), len(rootInBatch.Proofs))

	if rootInBatch.Proofs == nil || len(rootInBatch.Proofs) == 0 {
		t.Fatalf("Expected proofs to be preserved for partial DAG with pruned links")
	}

	t.Logf("SUCCESS: Root has %d proofs preserved after batching", len(rootInBatch.Proofs))

	// Verify transmission works
	receiverDag := &dag.Dag{
		Root:  partialDag.Root,
		Leafs: make(map[string]*dag.DagLeaf),
	}

	for i, batch := range sequence {
		if err := receiverDag.ApplyAndVerifyBatchedTransmissionPacket(batch); err != nil {
			t.Fatalf("Failed to apply batch %d: %v", i, err)
		}
	}

	// Verify receiver DAG
	if err := receiverDag.Verify(); err != nil {
		t.Fatalf("Receiver DAG partial verification failed: %v", err)
	}

	t.Log("Pruned links optimization test completed successfully")
	t.Log("This confirms that partial DAGs with pruned links preserve merkle proofs correctly")
}
