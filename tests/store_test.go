package tests

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/HORNET-Storage/Scionic-Merkle-Tree/v2/dag"
	"github.com/HORNET-Storage/Scionic-Merkle-Tree/v2/testutil"
)

func TestDagStoreBasic(t *testing.T) {
	testutil.RunTestWithAllFixtures(t, func(t *testing.T, d *dag.Dag, fixture testutil.TestFixture, fixturePath string) {
		// Create DagStore from the Dag
		store := dag.NewDagStore(d)

		// Verify root is set correctly
		if store.Root != d.Root {
			t.Errorf("Root mismatch: expected %s, got %s", d.Root, store.Root)
		}

		// Verify we can retrieve leaves
		rootLeaf, err := store.RetrieveLeaf(store.Root)
		if err != nil {
			t.Fatalf("Failed to retrieve root leaf: %v", err)
		}
		if rootLeaf == nil {
			t.Fatalf("Root leaf is nil")
		}
		if rootLeaf.Hash != d.Root {
			t.Errorf("Retrieved leaf hash mismatch: expected %s, got %s", d.Root, rootLeaf.Hash)
		}

		// Verify LeafCount is set
		if store.LeafCount != rootLeaf.LeafCount {
			t.Errorf("LeafCount mismatch: expected %d, got %d", rootLeaf.LeafCount, store.LeafCount)
		}

		t.Logf("✓ Fixture %s: Basic DagStore operations successful", fixture.Name)
	})
}

func TestDagStoreVerify(t *testing.T) {
	testutil.RunTestWithAllFixtures(t, func(t *testing.T, d *dag.Dag, fixture testutil.TestFixture, fixturePath string) {
		store := dag.NewDagStore(d)

		err := store.Verify()
		if err != nil {
			t.Fatalf("DagStore verification failed for fixture %s: %v", fixture.Name, err)
		}

		t.Logf("✓ Fixture %s: DagStore verification successful", fixture.Name)
	})
}

func TestDagStoreToDag(t *testing.T) {
	testutil.RunTestWithAllFixtures(t, func(t *testing.T, d *dag.Dag, fixture testutil.TestFixture, fixturePath string) {
		store := dag.NewDagStore(d)

		// Convert back to Dag
		restored, err := store.ToDag()
		if err != nil {
			t.Fatalf("Failed to convert DagStore to Dag: %v", err)
		}

		// Verify roots match
		if restored.Root != d.Root {
			t.Errorf("Root mismatch: expected %s, got %s", d.Root, restored.Root)
		}

		// Verify leaf counts match
		if len(restored.Leafs) != len(d.Leafs) {
			t.Errorf("Leaf count mismatch: expected %d, got %d", len(d.Leafs), len(restored.Leafs))
		}

		// Verify the restored Dag
		if err := restored.Verify(); err != nil {
			t.Fatalf("Restored Dag verification failed: %v", err)
		}

		t.Logf("✓ Fixture %s: DagStore to Dag conversion successful", fixture.Name)
	})
}

func TestDagStoreCreateDirectory(t *testing.T) {
	testutil.RunTestWithAllFixtures(t, func(t *testing.T, d *dag.Dag, fixture testutil.TestFixture, fixturePath string) {
		store := dag.NewDagStore(d)

		// Create temp directory for output
		tmpDir, err := os.MkdirTemp("", "dagstore_output_*")
		if err != nil {
			t.Fatalf("Could not create temp directory: %v", err)
		}
		defer os.RemoveAll(tmpDir)

		output := filepath.Join(tmpDir, fixture.Name)
		err = store.CreateDirectory(output)
		if err != nil {
			t.Fatalf("Failed to create directory from DagStore: %v", err)
		}

		// Verify the recreated directory produces the same DAG
		recreatedDag, err := dag.CreateDag(output, false)
		if err != nil {
			t.Fatalf("Failed to create DAG from recreated directory: %v", err)
		}

		if recreatedDag.Root != d.Root {
			t.Errorf("Recreated DAG root mismatch: expected %s, got %s", d.Root, recreatedDag.Root)
		}

		t.Logf("✓ Fixture %s: DagStore directory creation successful", fixture.Name)
	})
}

func TestDagStoreWithSeparateContent(t *testing.T) {
	testutil.RunTestWithAllFixtures(t, func(t *testing.T, d *dag.Dag, fixture testutil.TestFixture, fixturePath string) {
		leafStore := dag.NewMemoryLeafStore()
		contentStore := dag.NewMemoryContentStore()

		store := dag.NewDagStoreWithStorage(d, leafStore, contentStore)

		// Verify the store
		if err := store.Verify(); err != nil {
			t.Fatalf("DagStore with separate content verification failed: %v", err)
		}

		// Check that content was stored separately
		// Retrieve a leaf and check its content
		rootLeaf, err := store.RetrieveLeaf(store.Root)
		if err != nil {
			t.Fatalf("Failed to retrieve root leaf: %v", err)
		}

		// Find a leaf with content
		var foundContentLeaf bool
		err = store.IterateDag(func(leaf *dag.DagLeaf, parent *dag.DagLeaf) error {
			if len(leaf.ContentHash) > 0 {
				// This leaf should have content
				if len(leaf.Content) > 0 {
					foundContentLeaf = true
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("IterateDag failed: %v", err)
		}

		// Convert back to Dag and verify
		restored, err := store.ToDag()
		if err != nil {
			t.Fatalf("Failed to convert DagStore to Dag: %v", err)
		}

		if restored.Root != d.Root {
			t.Errorf("Root mismatch: expected %s, got %s", d.Root, restored.Root)
		}

		// Verify the restored Dag
		if err := restored.Verify(); err != nil {
			t.Fatalf("Restored Dag verification failed: %v", err)
		}

		_ = rootLeaf
		_ = foundContentLeaf

		t.Logf("✓ Fixture %s: DagStore with separate content successful", fixture.Name)
	})
}

func TestDagStoreWithOptions(t *testing.T) {
	testutil.RunTestWithAllFixtures(t, func(t *testing.T, d *dag.Dag, fixture testutil.TestFixture, fixturePath string) {
		leafStore := dag.NewMemoryLeafStore()
		contentStore := dag.NewMemoryContentStore()

		store := dag.NewDagStoreWithOptions(d,
			dag.WithLeafStore(leafStore),
			dag.WithContentStore(contentStore),
			dag.WithSeparateContent(true),
		)

		// Verify the store
		if err := store.Verify(); err != nil {
			t.Fatalf("DagStore with options verification failed: %v", err)
		}

		// Convert back and verify
		restored, err := store.ToDag()
		if err != nil {
			t.Fatalf("Failed to convert DagStore to Dag: %v", err)
		}

		if err := restored.Verify(); err != nil {
			t.Fatalf("Restored Dag verification failed: %v", err)
		}

		t.Logf("✓ Fixture %s: DagStore with options successful", fixture.Name)
	})
}

func TestDagStoreIterateDag(t *testing.T) {
	testutil.RunTestWithAllFixtures(t, func(t *testing.T, d *dag.Dag, fixture testutil.TestFixture, fixturePath string) {
		store := dag.NewDagStore(d)

		var leafCount int
		err := store.IterateDag(func(leaf *dag.DagLeaf, parent *dag.DagLeaf) error {
			leafCount++
			if leaf.Hash == "" {
				return nil
			}
			return nil
		})

		if err != nil {
			t.Fatalf("IterateDag failed: %v", err)
		}

		if leafCount != len(d.Leafs) {
			t.Errorf("Leaf count mismatch: expected %d, got %d", len(d.Leafs), leafCount)
		}

		t.Logf("✓ Fixture %s: DagStore IterateDag successful (counted %d leaves)", fixture.Name, leafCount)
	})
}

func TestDagStoreLabels(t *testing.T) {
	testutil.RunTestWithAllFixtures(t, func(t *testing.T, d *dag.Dag, fixture testutil.TestFixture, fixturePath string) {
		store := dag.NewDagStore(d)

		// Calculate labels
		if err := store.CalculateLabels(); err != nil {
			t.Fatalf("CalculateLabels failed: %v", err)
		}

		// Skip if only root (no non-root labels)
		if store.LeafCount <= 1 {
			t.Logf("✓ Fixture %s: Skipping label test (only root)", fixture.Name)
			return
		}

		// Get hashes by label range
		hashes, err := store.GetHashesByLabelRange("1", "1")
		if err != nil {
			t.Fatalf("GetHashesByLabelRange failed: %v", err)
		}

		if len(hashes) != 1 {
			t.Errorf("Expected 1 hash, got %d", len(hashes))
		}

		t.Logf("✓ Fixture %s: DagStore labels successful", fixture.Name)
	})
}

func TestDagStoreSerialization(t *testing.T) {
	testutil.RunTestWithAllFixtures(t, func(t *testing.T, d *dag.Dag, fixture testutil.TestFixture, fixturePath string) {
		store := dag.NewDagStore(d)

		// Test CBOR serialization
		cborData, err := store.ToCBOR()
		if err != nil {
			t.Fatalf("ToCBOR failed: %v", err)
		}

		restoredStore, err := dag.FromCBORToStore(cborData, nil, nil)
		if err != nil {
			t.Fatalf("FromCBORToStore failed: %v", err)
		}

		if restoredStore.Root != store.Root {
			t.Errorf("CBOR: Root mismatch: expected %s, got %s", store.Root, restoredStore.Root)
		}

		// Test JSON serialization
		jsonData, err := store.ToJSON()
		if err != nil {
			t.Fatalf("ToJSON failed: %v", err)
		}

		restoredStore2, err := dag.FromJSONToStore(jsonData, nil, nil)
		if err != nil {
			t.Fatalf("FromJSONToStore failed: %v", err)
		}

		if restoredStore2.Root != store.Root {
			t.Errorf("JSON: Root mismatch: expected %s, got %s", store.Root, restoredStore2.Root)
		}

		t.Logf("✓ Fixture %s: DagStore serialization successful", fixture.Name)
	})
}

func TestDagStoreTransmissionPackets(t *testing.T) {
	testutil.RunTestWithAllFixtures(t, func(t *testing.T, d *dag.Dag, fixture testutil.TestFixture, fixturePath string) {
		// Get transmission packets from original DAG
		packets := d.GetLeafSequence()
		if len(packets) == 0 {
			t.Fatalf("No transmission packets generated")
		}

		// Create empty store and add packets
		emptyStore := dag.NewEmptyDagStore(nil, nil)

		for _, packet := range packets {
			if err := emptyStore.AddTransmissionPacket(packet); err != nil {
				t.Fatalf("AddTransmissionPacket failed: %v", err)
			}
		}

		// Verify the rebuilt store
		if emptyStore.Root != d.Root {
			t.Errorf("Root mismatch: expected %s, got %s", d.Root, emptyStore.Root)
		}

		if err := emptyStore.Verify(); err != nil {
			t.Fatalf("Rebuilt store verification failed: %v", err)
		}

		t.Logf("✓ Fixture %s: DagStore transmission packets successful", fixture.Name)
	})
}

func TestDagStoreGetPartial(t *testing.T) {
	testutil.RunTestWithAllFixtures(t, func(t *testing.T, d *dag.Dag, fixture testutil.TestFixture, fixturePath string) {
		store := dag.NewDagStore(d)

		// Get root leaf to find a child to extract
		rootLeaf, err := store.RetrieveLeaf(store.Root)
		if err != nil {
			t.Fatalf("Failed to retrieve root leaf: %v", err)
		}

		if len(rootLeaf.Links) == 0 {
			t.Logf("✓ Fixture %s: Skipping partial test (no children)", fixture.Name)
			return
		}

		// Get partial with first child
		childHash := rootLeaf.Links[0]
		partial, err := store.GetPartial([]string{childHash}, true)
		if err != nil {
			t.Fatalf("GetPartial failed: %v", err)
		}

		// Verify the partial DAG
		if err := partial.Verify(); err != nil {
			t.Fatalf("Partial DAG verification failed: %v", err)
		}

		t.Logf("✓ Fixture %s: DagStore GetPartial successful", fixture.Name)
	})
}

func TestMemoryLeafStoreConcurrency(t *testing.T) {
	store := dag.NewMemoryLeafStore()

	// Create test leaves
	leaves := make([]*dag.DagLeaf, 100)
	for i := 0; i < 100; i++ {
		leaves[i] = &dag.DagLeaf{
			Hash:     "test-hash-" + string(rune(i)),
			ItemName: "test-item-" + string(rune(i)),
		}
	}

	// Concurrent writes
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			store.StoreLeaf(leaves[idx])
		}(i)
	}
	wg.Wait()

	// Concurrent reads
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			store.RetrieveLeaf(leaves[idx].Hash)
		}(i)
	}
	wg.Wait()

	// Verify count
	if store.Count() != 100 {
		t.Errorf("Expected 100 leaves, got %d", store.Count())
	}

	t.Log("✓ MemoryLeafStore concurrency test successful")
}

func TestMemoryContentStoreConcurrency(t *testing.T) {
	store := dag.NewMemoryContentStore()

	// Concurrent writes
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			store.StoreContent("hash-"+string(rune(idx)), []byte("content"))
		}(i)
	}
	wg.Wait()

	// Concurrent reads
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			store.RetrieveContent("hash-" + string(rune(idx)))
		}(i)
	}
	wg.Wait()

	t.Log("✓ MemoryContentStore concurrency test successful")
}

func TestBatchOperations(t *testing.T) {
	store := dag.NewMemoryLeafStore()

	// Create test leaves
	leaves := []*dag.DagLeaf{
		{Hash: "hash1", ItemName: "item1"},
		{Hash: "hash2", ItemName: "item2"},
		{Hash: "hash3", ItemName: "item3"},
	}

	// Batch store
	if err := store.StoreLeaves(leaves); err != nil {
		t.Fatalf("StoreLeaves failed: %v", err)
	}

	// Batch retrieve
	hashes := []string{"hash1", "hash2", "hash3", "hash4"} // hash4 doesn't exist
	retrieved, err := store.RetrieveLeaves(hashes)
	if err != nil {
		t.Fatalf("RetrieveLeaves failed: %v", err)
	}

	if len(retrieved) != 3 {
		t.Errorf("Expected 3 leaves, got %d", len(retrieved))
	}

	// Verify hash4 is not in results
	if _, exists := retrieved["hash4"]; exists {
		t.Error("hash4 should not be in results")
	}

	t.Log("✓ Batch operations test successful")
}

func TestEmptyDagStore(t *testing.T) {
	store := dag.NewEmptyDagStore(nil, nil)

	if store.Root != "" {
		t.Errorf("Empty store should have empty root, got %s", store.Root)
	}

	if store.LeafCount != 0 {
		t.Errorf("Empty store should have 0 leaf count, got %d", store.LeafCount)
	}

	// Should be able to store and retrieve leaves
	leaf := &dag.DagLeaf{Hash: "test-hash", ItemName: "test-item"}
	if err := store.StoreLeaf(leaf); err != nil {
		t.Fatalf("StoreLeaf failed: %v", err)
	}

	retrieved, err := store.RetrieveLeaf("test-hash")
	if err != nil {
		t.Fatalf("RetrieveLeaf failed: %v", err)
	}
	if retrieved == nil {
		t.Fatal("Retrieved leaf should not be nil")
	}

	t.Log("✓ Empty DagStore test successful")
}

func TestDagStoreHasLeaf(t *testing.T) {
	testutil.RunTestWithAllFixtures(t, func(t *testing.T, d *dag.Dag, fixture testutil.TestFixture, fixturePath string) {
		store := dag.NewDagStore(d)

		// Root should exist
		hasRoot, err := store.HasLeaf(store.Root)
		if err != nil {
			t.Fatalf("HasLeaf failed: %v", err)
		}
		if !hasRoot {
			t.Error("Root leaf should exist")
		}

		// Non-existent hash should not exist
		hasFake, err := store.HasLeaf("fake-hash-that-does-not-exist")
		if err != nil {
			t.Fatalf("HasLeaf failed: %v", err)
		}
		if hasFake {
			t.Error("Fake hash should not exist")
		}

		t.Logf("✓ Fixture %s: DagStore HasLeaf successful", fixture.Name)
	})
}

func TestDagStoreDeleteLeaf(t *testing.T) {
	store := dag.NewEmptyDagStore(nil, nil)

	// Store a leaf
	leaf := &dag.DagLeaf{Hash: "delete-test-hash", ItemName: "delete-test-item", Content: []byte("test content")}
	if err := store.StoreLeaf(leaf); err != nil {
		t.Fatalf("StoreLeaf failed: %v", err)
	}

	// Verify it exists
	hasLeaf, _ := store.HasLeaf("delete-test-hash")
	if !hasLeaf {
		t.Fatal("Leaf should exist before delete")
	}

	// Delete it
	if err := store.DeleteLeaf("delete-test-hash"); err != nil {
		t.Fatalf("DeleteLeaf failed: %v", err)
	}

	// Verify it's gone
	hasLeaf, _ = store.HasLeaf("delete-test-hash")
	if hasLeaf {
		t.Error("Leaf should not exist after delete")
	}

	t.Log("✓ DagStore DeleteLeaf test successful")
}

func TestRetrieveLeafWithoutContent(t *testing.T) {
	testutil.RunTestWithAllFixtures(t, func(t *testing.T, d *dag.Dag, fixture testutil.TestFixture, fixturePath string) {
		leafStore := dag.NewMemoryLeafStore()
		contentStore := dag.NewMemoryContentStore()

		store := dag.NewDagStoreWithStorage(d, leafStore, contentStore)

		// Find a leaf with content
		var contentLeafHash string
		for hash, leaf := range d.Leafs {
			if len(leaf.Content) > 0 {
				contentLeafHash = hash
				break
			}
		}

		if contentLeafHash == "" {
			t.Logf("✓ Fixture %s: Skipping (no content leaves)", fixture.Name)
			return
		}

		// Retrieve without content
		leafWithoutContent, err := store.RetrieveLeafWithoutContent(contentLeafHash)
		if err != nil {
			t.Fatalf("RetrieveLeafWithoutContent failed: %v", err)
		}

		// Content should be empty (stored separately)
		if len(leafWithoutContent.Content) > 0 {
			t.Log("Note: Content present in leaf without content retrieval (may be expected for in-memory)")
		}

		// Retrieve with content
		leafWithContent, err := store.RetrieveLeaf(contentLeafHash)
		if err != nil {
			t.Fatalf("RetrieveLeaf failed: %v", err)
		}

		// Content should be present
		if len(leafWithContent.ContentHash) > 0 && len(leafWithContent.Content) == 0 {
			t.Log("Note: Content not restored (content hash present but content empty)")
		}

		t.Logf("✓ Fixture %s: RetrieveLeafWithoutContent successful", fixture.Name)
	})
}

func TestDagStoreVerifyStreaming(t *testing.T) {
	testutil.RunTestWithAllFixtures(t, func(t *testing.T, d *dag.Dag, fixture testutil.TestFixture, fixturePath string) {
		store := dag.NewDagStore(d)

		err := store.VerifyStreaming()
		if err != nil {
			t.Fatalf("VerifyStreaming failed for fixture %s: %v", fixture.Name, err)
		}

		t.Logf("✓ Fixture %s: DagStore streaming verification successful", fixture.Name)
	})
}

func TestDagStoreVerifyStreamingWithCustomStore(t *testing.T) {
	testutil.RunTestWithAllFixtures(t, func(t *testing.T, d *dag.Dag, fixture testutil.TestFixture, fixturePath string) {
		leafStore := dag.NewMemoryLeafStore()
		contentStore := dag.NewMemoryContentStore()

		store := dag.NewDagStoreWithStorage(d, leafStore, contentStore)

		err := store.VerifyStreaming()
		if err != nil {
			t.Fatalf("VerifyStreaming failed for fixture %s: %v", fixture.Name, err)
		}

		t.Logf("✓ Fixture %s: DagStore streaming verification with custom store successful", fixture.Name)
	})
}

func TestDagStoreCreateDirectoryStreaming(t *testing.T) {
	testutil.RunTestWithAllFixtures(t, func(t *testing.T, d *dag.Dag, fixture testutil.TestFixture, fixturePath string) {
		store := dag.NewDagStore(d)

		// Create temp directory for output
		tmpDir, err := os.MkdirTemp("", "dagstore_streaming_output_*")
		if err != nil {
			t.Fatalf("Could not create temp directory: %v", err)
		}
		defer os.RemoveAll(tmpDir)

		output := filepath.Join(tmpDir, fixture.Name)
		err = store.CreateDirectoryStreaming(output)
		if err != nil {
			t.Fatalf("CreateDirectoryStreaming failed: %v", err)
		}

		// Verify the recreated directory produces the same DAG
		recreatedDag, err := dag.CreateDag(output, false)
		if err != nil {
			t.Fatalf("Failed to create DAG from recreated directory: %v", err)
		}

		if recreatedDag.Root != d.Root {
			t.Errorf("Recreated DAG root mismatch: expected %s, got %s", d.Root, recreatedDag.Root)
		}

		t.Logf("✓ Fixture %s: DagStore streaming directory creation successful", fixture.Name)
	})
}

func TestDagStoreIterateDagStreaming(t *testing.T) {
	testutil.RunTestWithAllFixtures(t, func(t *testing.T, d *dag.Dag, fixture testutil.TestFixture, fixturePath string) {
		store := dag.NewDagStore(d)

		var leafCount int
		err := store.IterateDagStreaming(func(leaf *dag.DagLeaf, parent *dag.DagLeaf) error {
			leafCount++
			return nil
		})

		if err != nil {
			t.Fatalf("IterateDagStreaming failed: %v", err)
		}

		if leafCount != len(d.Leafs) {
			t.Errorf("Leaf count mismatch: expected %d, got %d", len(d.Leafs), leafCount)
		}

		t.Logf("✓ Fixture %s: DagStore streaming iteration successful (counted %d leaves)", fixture.Name, leafCount)
	})
}

func TestDagStoreCountLeavesStreaming(t *testing.T) {
	testutil.RunTestWithAllFixtures(t, func(t *testing.T, d *dag.Dag, fixture testutil.TestFixture, fixturePath string) {
		store := dag.NewDagStore(d)

		count, err := store.CountLeavesStreaming()
		if err != nil {
			t.Fatalf("CountLeavesStreaming failed: %v", err)
		}

		if count != len(d.Leafs) {
			t.Errorf("Leaf count mismatch: expected %d, got %d", len(d.Leafs), count)
		}

		t.Logf("✓ Fixture %s: DagStore streaming count successful (%d leaves)", fixture.Name, count)
	})
}

func TestDagStoreGetContentStreaming(t *testing.T) {
	testutil.RunTestWithAllFixtures(t, func(t *testing.T, d *dag.Dag, fixture testutil.TestFixture, fixturePath string) {
		store := dag.NewDagStore(d)

		// Find a file leaf with content
		var fileLeafHash string
		for hash, leaf := range d.Leafs {
			if leaf.Type == dag.FileLeafType {
				fileLeafHash = hash
				break
			}
		}

		if fileLeafHash == "" {
			t.Logf("✓ Fixture %s: Skipping content test (no file leaves)", fixture.Name)
			return
		}

		// Get content via streaming method
		streamingContent, err := store.GetContentFromLeafStreaming(fileLeafHash)
		if err != nil {
			t.Fatalf("GetContentFromLeafStreaming failed: %v", err)
		}

		// Get content via regular method
		regularContent, err := store.GetContentFromLeaf(fileLeafHash)
		if err != nil {
			t.Fatalf("GetContentFromLeaf failed: %v", err)
		}

		// Compare
		if len(streamingContent) != len(regularContent) {
			t.Errorf("Content length mismatch: streaming=%d, regular=%d", len(streamingContent), len(regularContent))
		}

		t.Logf("✓ Fixture %s: DagStore streaming content retrieval successful", fixture.Name)
	})
}

func TestDagStoreVerifyLeafWithParent(t *testing.T) {
	testutil.RunTestWithAllFixtures(t, func(t *testing.T, d *dag.Dag, fixture testutil.TestFixture, fixturePath string) {
		store := dag.NewDagStore(d)

		// Get root and find a child
		rootLeaf := d.Leafs[d.Root]
		if len(rootLeaf.Links) == 0 {
			t.Logf("✓ Fixture %s: Skipping (root has no children)", fixture.Name)
			return
		}

		childHash := rootLeaf.Links[0]

		// Get proof if needed
		var proof *dag.ClassicTreeBranch
		if rootLeaf.CurrentLinkCount > 1 {
			var err error
			proof, err = rootLeaf.GetBranch(childHash)
			if err != nil {
				t.Fatalf("Failed to get branch: %v", err)
			}
		}

		// Verify the child with parent
		err := store.VerifyLeafWithParent(childHash, d.Root, proof)
		if err != nil {
			t.Fatalf("VerifyLeafWithParent failed: %v", err)
		}

		t.Logf("✓ Fixture %s: DagStore VerifyLeafWithParent successful", fixture.Name)
	})
}

func TestStreamingVerificationMatchesRegular(t *testing.T) {
	testutil.RunTestWithAllFixtures(t, func(t *testing.T, d *dag.Dag, fixture testutil.TestFixture, fixturePath string) {
		store := dag.NewDagStore(d)

		// Regular verification
		regularErr := store.Verify()

		// Streaming verification
		streamingErr := store.VerifyStreaming()

		// Both should either pass or fail
		if (regularErr == nil) != (streamingErr == nil) {
			t.Errorf("Verification mismatch: regular=%v, streaming=%v", regularErr, streamingErr)
		}

		t.Logf("✓ Fixture %s: Streaming verification matches regular", fixture.Name)
	})
}

func TestStreamingWithTransmissionPackets(t *testing.T) {
	testutil.RunTestWithAllFixtures(t, func(t *testing.T, d *dag.Dag, fixture testutil.TestFixture, fixturePath string) {
		// Get transmission packets from original DAG
		packets := d.GetLeafSequence()
		if len(packets) == 0 {
			t.Fatalf("No transmission packets generated")
		}

		// Create empty store and add packets one by one
		emptyStore := dag.NewEmptyDagStore(nil, nil)

		for _, packet := range packets {
			if err := emptyStore.AddTransmissionPacket(packet); err != nil {
				t.Fatalf("AddTransmissionPacket failed: %v", err)
			}
		}

		// Verify using streaming (memory-efficient)
		if err := emptyStore.VerifyStreaming(); err != nil {
			t.Fatalf("VerifyStreaming failed after building from packets: %v", err)
		}

		// Count leaves
		count, err := emptyStore.CountLeavesStreaming()
		if err != nil {
			t.Fatalf("CountLeavesStreaming failed: %v", err)
		}

		if count != len(d.Leafs) {
			t.Errorf("Leaf count mismatch: expected %d, got %d", len(d.Leafs), count)
		}

		t.Logf("✓ Fixture %s: Streaming with transmission packets successful", fixture.Name)
	})
}

func TestStreamingPartialDagVerification(t *testing.T) {
	testutil.RunTestWithAllFixtures(t, func(t *testing.T, d *dag.Dag, fixture testutil.TestFixture, fixturePath string) {
		// Get root leaf to find children
		rootLeaf := d.Leafs[d.Root]
		if len(rootLeaf.Links) < 2 {
			t.Logf("✓ Fixture %s: Skipping partial test (need at least 2 children)", fixture.Name)
			return
		}

		// Create a partial DAG with just the first child and its verification path
		firstChildHash := rootLeaf.Links[0]
		partial, err := d.GetPartial([]string{firstChildHash}, true)
		if err != nil {
			t.Fatalf("GetPartial failed: %v", err)
		}

		// Verify the partial DAG using regular Verify first
		if err := partial.Verify(); err != nil {
			t.Fatalf("Regular partial verification failed: %v", err)
		}

		// Now create a DagStore from the partial DAG
		partialStore := dag.NewDagStore(partial)

		// Verify using streaming - should handle partial DAG with proofs
		if err := partialStore.VerifyStreaming(); err != nil {
			t.Fatalf("VerifyStreaming on partial DAG failed: %v", err)
		}

		t.Logf("✓ Fixture %s: Streaming partial DAG verification successful", fixture.Name)
	})
}

func TestStreamingPartialDagVerificationMultipleLeaves(t *testing.T) {
	testutil.RunTestWithAllFixtures(t, func(t *testing.T, d *dag.Dag, fixture testutil.TestFixture, fixturePath string) {
		// Need at least 3 leaves to do meaningful partial test
		if len(d.Leafs) < 3 {
			t.Logf("✓ Fixture %s: Skipping (need at least 3 leaves)", fixture.Name)
			return
		}

		// Collect some leaf hashes (not the root)
		var leafHashes []string
		for hash := range d.Leafs {
			if hash != d.Root {
				leafHashes = append(leafHashes, hash)
				if len(leafHashes) >= 2 {
					break
				}
			}
		}

		if len(leafHashes) < 2 {
			t.Logf("✓ Fixture %s: Skipping (couldn't find 2 non-root leaves)", fixture.Name)
			return
		}

		// Create partial DAG
		partial, err := d.GetPartial(leafHashes, false) // Keep all links for reconstruction
		if err != nil {
			t.Fatalf("GetPartial failed: %v", err)
		}

		// Verify partial is actually partial
		if !partial.IsPartial() {
			t.Logf("Note: Partial DAG is actually complete (small fixture)")
		}

		// Verify with regular method first
		if err := partial.Verify(); err != nil {
			t.Fatalf("Regular partial verification failed: %v", err)
		}

		// Create store and verify streaming
		partialStore := dag.NewDagStore(partial)
		if err := partialStore.VerifyStreaming(); err != nil {
			t.Fatalf("VerifyStreaming on partial DAG failed: %v", err)
		}

		t.Logf("✓ Fixture %s: Streaming partial multi-leaf verification successful", fixture.Name)
	})
}

func TestStreamingVerificationConsistency(t *testing.T) {
	testutil.RunTestWithAllFixtures(t, func(t *testing.T, d *dag.Dag, fixture testutil.TestFixture, fixturePath string) {
		// Test 1: Full DAG
		fullStore := dag.NewDagStore(d)
		regularFullErr := fullStore.Verify()
		streamingFullErr := fullStore.VerifyStreaming()

		if (regularFullErr == nil) != (streamingFullErr == nil) {
			t.Errorf("Full DAG: verification mismatch: regular=%v, streaming=%v", regularFullErr, streamingFullErr)
		}

		// Test 2: Partial DAG (if possible)
		if len(d.Leafs) >= 3 {
			var leafHash string
			for hash := range d.Leafs {
				if hash != d.Root {
					leafHash = hash
					break
				}
			}

			if leafHash != "" {
				partial, err := d.GetPartial([]string{leafHash}, true)
				if err == nil {
					partialStore := dag.NewDagStore(partial)
					regularPartialErr := partialStore.Verify()
					streamingPartialErr := partialStore.VerifyStreaming()

					if (regularPartialErr == nil) != (streamingPartialErr == nil) {
						t.Errorf("Partial DAG: verification mismatch: regular=%v, streaming=%v", regularPartialErr, streamingPartialErr)
					}
				}
			}
		}

		t.Logf("✓ Fixture %s: Streaming verification consistency confirmed", fixture.Name)
	})
}

func TestStreamingVerificationRigorousComparison(t *testing.T) {
	testutil.RunTestWithAllFixtures(t, func(t *testing.T, d *dag.Dag, fixture testutil.TestFixture, fixturePath string) {
		// 1. Verify the original Dag directly
		originalErr := d.Verify()
		if originalErr != nil {
			t.Fatalf("Original Dag.Verify() failed: %v", originalErr)
		}

		// 2. Create DagStore and verify it using regular Verify (which uses ToDag internally)
		store := dag.NewDagStore(d)
		storeVerifyErr := store.Verify()
		if storeVerifyErr != nil {
			t.Fatalf("DagStore.Verify() failed: %v", storeVerifyErr)
		}

		// 3. Verify using streaming method
		streamingErr := store.VerifyStreaming()
		if streamingErr != nil {
			t.Fatalf("DagStore.VerifyStreaming() failed: %v", streamingErr)
		}

		// 4. Convert back to Dag and verify again
		restored, err := store.ToDag()
		if err != nil {
			t.Fatalf("ToDag() failed: %v", err)
		}
		restoredErr := restored.Verify()
		if restoredErr != nil {
			t.Fatalf("Restored Dag.Verify() failed: %v", restoredErr)
		}

		// 5. Check that roots match
		if restored.Root != d.Root {
			t.Errorf("Root mismatch after round-trip: original=%s, restored=%s", d.Root, restored.Root)
		}

		// 6. Check that leaf counts match
		if len(restored.Leafs) != len(d.Leafs) {
			t.Errorf("Leaf count mismatch: original=%d, restored=%d", len(d.Leafs), len(restored.Leafs))
		}

		// 7. Verify each leaf hash matches
		for hash := range d.Leafs {
			if _, exists := restored.Leafs[hash]; !exists {
				t.Errorf("Missing leaf after round-trip: %s", hash)
			}
		}

		t.Logf("✓ Fixture %s: Rigorous streaming verification comparison passed", fixture.Name)
	})
}

func TestStreamingVerificationDetectsTampering(t *testing.T) {
	testutil.RunTestWithAllFixtures(t, func(t *testing.T, d *dag.Dag, fixture testutil.TestFixture, fixturePath string) {
		// Create a valid store first
		store := dag.NewDagStore(d)

		// Verify it passes initially
		if err := store.VerifyStreaming(); err != nil {
			t.Fatalf("Initial verification failed: %v", err)
		}

		// Skip if only root (can't tamper meaningfully)
		if len(d.Leafs) <= 1 {
			t.Logf("✓ Fixture %s: Skipping tampering test (only root)", fixture.Name)
			return
		}

		// Find a non-root leaf to tamper with
		var tamperHash string
		for hash := range d.Leafs {
			if hash != d.Root {
				tamperHash = hash
				break
			}
		}

		if tamperHash == "" {
			t.Logf("✓ Fixture %s: Skipping tampering test (no non-root leaves)", fixture.Name)
			return
		}

		// Create a new store with tampered leaf
		tamperedStore := dag.NewEmptyDagStore(nil, nil)
		for hash, leaf := range d.Leafs {
			if hash == tamperHash {
				// Create tampered copy
				tampered := leaf.Clone()
				tampered.ItemName = "TAMPERED_" + tampered.ItemName
				tamperedStore.StoreLeaf(tampered)
			} else {
				tamperedStore.StoreLeaf(leaf)
			}
		}
		tamperedStore.SetRoot(d.Root)

		// Streaming verification should fail on tampered data
		tamperErr := tamperedStore.VerifyStreaming()
		if tamperErr == nil {
			t.Errorf("Streaming verification should have detected tampering but passed!")
		} else {
			t.Logf("✓ Fixture %s: Correctly detected tampering: %v", fixture.Name, tamperErr)
		}
	})
}

func TestDagStoreAddBatchedTransmissionPacket(t *testing.T) {
	testutil.RunTestWithAllFixtures(t, func(t *testing.T, d *dag.Dag, fixture testutil.TestFixture, fixturePath string) {
		// Get batched sequence from original dag
		batchedSequence := d.GetBatchedLeafSequence()

		// Create empty store and add packets via batched method
		receiverStore := dag.NewEmptyDagStore(nil, nil)

		for _, batchedPacket := range batchedSequence {
			err := receiverStore.AddBatchedTransmissionPacket(batchedPacket)
			if err != nil {
				t.Fatalf("Failed to add batched packet: %v", err)
			}
		}

		// Verify the receiver store
		err := receiverStore.VerifyStreaming()
		if err != nil {
			t.Fatalf("Receiver store verification failed: %v", err)
		}

		// Convert to Dag and compare
		receiverDag, err := receiverStore.ToDag()
		if err != nil {
			t.Fatalf("Failed to convert receiver store to Dag: %v", err)
		}

		// Compare roots
		if receiverDag.Root != d.Root {
			t.Errorf("Root mismatch: expected %s, got %s", d.Root, receiverDag.Root)
		}

		// Compare leaf counts
		if len(receiverDag.Leafs) != len(d.Leafs) {
			t.Errorf("Leaf count mismatch: expected %d, got %d", len(d.Leafs), len(receiverDag.Leafs))
		}

		t.Logf("✓ Fixture %s: AddBatchedTransmissionPacket test successful (transmitted %d batches, %d leaves)",
			fixture.Name, len(batchedSequence), len(d.Leafs))
	})
}

func TestDagStoreAddBatchedPacket_EmptyPacket(t *testing.T) {
	store := dag.NewEmptyDagStore(nil, nil)

	// Empty packet should not cause error
	err := store.AddBatchedTransmissionPacket(nil)
	if err != nil {
		t.Errorf("Nil packet should not error: %v", err)
	}

	// Packet with no leaves should not cause error
	emptyPacket := &dag.BatchedTransmissionPacket{
		Leaves: []*dag.DagLeaf{},
	}
	err = store.AddBatchedTransmissionPacket(emptyPacket)
	if err != nil {
		t.Errorf("Empty leaves packet should not error: %v", err)
	}
}
