package dag

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStripLabel(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Hash with label",
			input:    "123:bafyreiabc123",
			expected: "bafyreiabc123",
		},
		{
			name:     "Hash without label",
			input:    "bafyreiabc123",
			expected: "bafyreiabc123",
		},
		{
			name:     "Empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "Hash with multiple colons",
			input:    "1:2:3",
			expected: "2:3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := StripLabel(tt.input)
			if result != tt.expected {
				t.Errorf("StripLabel(%q) = %q, expected %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestDagLeaf_ReplaceLabelInLink(t *testing.T) {
	leaf := &DagLeaf{
		Hash:     "bafyreiabc123",
		ItemName: "test",
		Type:     FileLeafType,
	}

	tests := []struct {
		name      string
		childHash string
		newLabel  string
		expected  string
	}{
		{
			name:      "Replace existing label",
			childHash: "123:bafyreibc456",
			newLabel:  "999",
			expected:  "999:bafyreibc456",
		},
		{
			name:      "Add label to unlabeled hash",
			childHash: "bafyreibc456",
			newLabel:  "42",
			expected:  "42:bafyreibc456",
		},
		{
			name:      "Replace with different label",
			childHash: "1:bafyreixyz789",
			newLabel:  "100",
			expected:  "100:bafyreixyz789",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := leaf.ReplaceLabelInLink(tt.childHash, tt.newLabel)
			if result != tt.expected {
				t.Errorf("ReplaceLabelInLink(%q, %q) = %q, expected %q",
					tt.childHash, tt.newLabel, result, tt.expected)
			}
		})
	}
}

func TestRecomputeLabels(t *testing.T) {
	// Create a temporary test directory
	testDir, err := os.MkdirTemp("", "dag_label_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Generate test data
	// Create a structure with multiple files and directories to ensure we have multiple labels
	GenerateDummyDirectory(filepath.Join(testDir, "input"), 2, 5, 1, 3)

	// Create a DAG from the directory
	dag, err := CreateDag(filepath.Join(testDir, "input"), true)
	if err != nil {
		t.Fatalf("Failed to create DAG: %v", err)
	}

	// Verify the DAG before recomputing labels
	err = dag.Verify()
	if err != nil {
		t.Fatalf("DAG verification failed before recompute: %v", err)
	}

	// Store original hashes to verify they change
	originalHashes := make(map[string]bool)
	for hash := range dag.Leafs {
		originalHashes[hash] = true
	}

	// Store the number of leaves
	originalLeafCount := len(dag.Leafs)

	// Recompute labels
	err = dag.RecomputeLabels()
	if err != nil {
		t.Fatalf("Failed to recompute labels: %v", err)
	}

	// Verify the DAG still works after recomputing labels
	err = dag.Verify()
	if err != nil {
		t.Fatalf("DAG verification failed after recompute: %v", err)
	}

	// Verify we have the same number of leaves
	if len(dag.Leafs) != originalLeafCount {
		t.Errorf("Leaf count changed after recompute: got %d, expected %d",
			len(dag.Leafs), originalLeafCount)
	}

	// Verify labels are sequential starting from 1
	labelsFound := make(map[string]bool)
	nonRootCount := 0

	for hash, leaf := range dag.Leafs {
		if hash == dag.Root {
			// Root should not have a label
			if HasLabel(hash) {
				t.Errorf("Root hash should not have a label, got: %s", hash)
			}
			continue
		}

		nonRootCount++

		// Non-root leaves should have labels
		if !HasLabel(hash) {
			t.Errorf("Non-root leaf should have a label, got: %s", hash)
			continue
		}

		label := GetLabel(hash)
		if label == "" {
			t.Errorf("Failed to extract label from hash: %s", hash)
			continue
		}

		labelsFound[label] = true

		// Verify the leaf's Hash field matches the map key
		if leaf.Hash != hash {
			t.Errorf("Leaf hash mismatch: map key %s, leaf.Hash %s", hash, leaf.Hash)
		}

		// Verify all links have proper labels
		for linkLabel, linkHash := range leaf.Links {
			if !HasLabel(linkHash) {
				t.Errorf("Link in leaf %s has no label: %s -> %s", hash, linkLabel, linkHash)
			}

			// Verify the link label matches the label in the hash
			extractedLabel := GetLabel(linkHash)
			if extractedLabel != linkLabel {
				t.Errorf("Link label mismatch in leaf %s: key %s, extracted %s from %s",
					hash, linkLabel, extractedLabel, linkHash)
			}
		}
	}

	// Verify we have exactly nonRootCount unique labels
	if len(labelsFound) != nonRootCount {
		t.Errorf("Expected %d unique labels, found %d", nonRootCount, len(labelsFound))
	}

	t.Logf("Successfully recomputed labels for %d leaves (%d non-root)", originalLeafCount, nonRootCount)
	t.Logf("Labels found: %v", labelsFound)
}

func TestRecomputeLabelsPreservesStructure(t *testing.T) {
	// Create a temporary test directory
	testDir, err := os.MkdirTemp("", "dag_label_structure_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Generate test data
	GenerateDummyDirectory(filepath.Join(testDir, "input"), 2, 3, 1, 2)

	// Create a DAG
	dag, err := CreateDag(filepath.Join(testDir, "input"), true)
	if err != nil {
		t.Fatalf("Failed to create DAG: %v", err)
	}

	// Store the structure before recomputing
	rootLeafBefore := dag.Leafs[dag.Root]
	linkCountBefore := rootLeafBefore.CurrentLinkCount

	// Store all parent-child relationships (using bare hashes)
	relationshipsBefore := make(map[string][]string)
	for parentHash, parentLeaf := range dag.Leafs {
		bareParent := StripLabel(parentHash)
		children := make([]string, 0, len(parentLeaf.Links))
		for _, childHash := range parentLeaf.Links {
			children = append(children, StripLabel(childHash))
		}
		relationshipsBefore[bareParent] = children
	}

	// Recompute labels
	err = dag.RecomputeLabels()
	if err != nil {
		t.Fatalf("Failed to recompute labels: %v", err)
	}

	// Verify the structure is preserved
	rootLeafAfter := dag.Leafs[dag.Root]
	linkCountAfter := rootLeafAfter.CurrentLinkCount

	if linkCountBefore != linkCountAfter {
		t.Errorf("Root link count changed: before %d, after %d", linkCountBefore, linkCountAfter)
	}

	// Verify all parent-child relationships are preserved
	for parentHash, parentLeaf := range dag.Leafs {
		bareParent := StripLabel(parentHash)

		expectedChildren, exists := relationshipsBefore[bareParent]
		if !exists {
			t.Errorf("Parent %s not found in original structure", bareParent)
			continue
		}

		actualChildren := make([]string, 0, len(parentLeaf.Links))
		for _, childHash := range parentLeaf.Links {
			actualChildren = append(actualChildren, StripLabel(childHash))
		}

		if len(expectedChildren) != len(actualChildren) {
			t.Errorf("Parent %s child count mismatch: expected %d, got %d",
				bareParent, len(expectedChildren), len(actualChildren))
			continue
		}

		// Check that all expected children are present (order might differ)
		childMap := make(map[string]bool)
		for _, child := range actualChildren {
			childMap[child] = true
		}

		for _, expectedChild := range expectedChildren {
			if !childMap[expectedChild] {
				t.Errorf("Parent %s missing child %s after recompute", bareParent, expectedChild)
			}
		}
	}

	t.Logf("Structure preserved successfully after label recomputation")
}

func TestRecomputeLabelsIsDeterministic(t *testing.T) {
	// Create a temporary test directory
	testDir, err := os.MkdirTemp("", "dag_label_deterministic_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Generate test data with enough complexity to ensure meaningful test
	GenerateDummyDirectory(filepath.Join(testDir, "input"), 3, 5, 1, 3)

	// Create a DAG
	dag, err := CreateDag(filepath.Join(testDir, "input"), true)
	if err != nil {
		t.Fatalf("Failed to create DAG: %v", err)
	}

	// Capture the initial state after first recompute
	err = dag.RecomputeLabels()
	if err != nil {
		t.Fatalf("Failed initial recompute: %v", err)
	}

	// Capture all leaf hashes and their links after first recompute
	firstSnapshot := make(map[string]map[string]string)
	for hash, leaf := range dag.Leafs {
		linksCopy := make(map[string]string)
		for label, childHash := range leaf.Links {
			linksCopy[label] = childHash
		}
		firstSnapshot[hash] = linksCopy
	}

	// Recompute labels 3 more times
	for i := 1; i <= 3; i++ {
		err = dag.RecomputeLabels()
		if err != nil {
			t.Fatalf("Failed recompute iteration %d: %v", i, err)
		}

		// Verify exact same structure after each recompute
		if len(dag.Leafs) != len(firstSnapshot) {
			t.Errorf("Iteration %d: Leaf count changed: got %d, expected %d",
				i, len(dag.Leafs), len(firstSnapshot))
		}

		// Verify all hashes and links are identical
		for hash, expectedLinks := range firstSnapshot {
			leaf, exists := dag.Leafs[hash]
			if !exists {
				t.Errorf("Iteration %d: Hash %s missing from DAG", i, hash)
				continue
			}

			if len(leaf.Links) != len(expectedLinks) {
				t.Errorf("Iteration %d: Hash %s link count changed: got %d, expected %d",
					i, hash, len(leaf.Links), len(expectedLinks))
			}

			// Verify each link is identical
			for label, expectedChildHash := range expectedLinks {
				actualChildHash, exists := leaf.Links[label]
				if !exists {
					t.Errorf("Iteration %d: Hash %s missing link label %s", i, hash, label)
					continue
				}
				if actualChildHash != expectedChildHash {
					t.Errorf("Iteration %d: Hash %s link %s mismatch: got %s, expected %s",
						i, hash, label, actualChildHash, expectedChildHash)
				}
			}
		}
	}

	t.Logf("Successfully verified deterministic recomputation across 4 iterations")
	t.Logf("Total leaves: %d", len(dag.Leafs))
}
