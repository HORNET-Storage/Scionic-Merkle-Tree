package dag

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGetPartial(t *testing.T) {
	// Create a temporary test directory
	testDir, err := os.MkdirTemp("", "dag_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(testDir) // Clean up after test

	// Generate test data with a known structure:
	// - 5 items max per directory
	// - 3 levels deep
	// This ensures we have enough files to test partial DAG retrieval
	GenerateDummyDirectory(filepath.Join(testDir, "input"), 3, 5, 1, 3)

	// Create a test DAG from the directory with timestamp to ensure root is built correctly
	dag, err := CreateDag(filepath.Join(testDir, "input"), true)
	if err != nil {
		t.Fatalf("Failed to create DAG: %v", err)
	}

	// Verify the DAG was created correctly
	err = dag.Verify()
	if err != nil {
		t.Fatalf("DAG verification failed: %v", err)
	}

	// Count file leaves to ensure we have enough for testing
	var fileCount int
	for _, leaf := range dag.Leafs {
		if leaf.Type == FileLeafType {
			fileCount++
		}
	}
	if fileCount < 3 {
		t.Fatalf("Not enough file leaves in the DAG, got %d", fileCount)
	}

	// Test getting a partial DAG with the first three leaves
	partial, err := dag.GetPartial(1, 3)
	if err != nil {
		t.Fatalf("Failed to get partial DAG: %v", err)
	}

	// Verify the partial DAG
	err = partial.Verify()
	if err != nil {
		t.Errorf("Partial DAG verification failed: %v", err)
	}

	// Test invalid range
	_, err = dag.GetPartial(1000, 2000)
	if err == nil {
		t.Error("GetPartial should handle invalid range gracefully")
	}
}

func TestGetPartialSingleLeaf(t *testing.T) {
	// Create a temporary test directory
	testDir, err := os.MkdirTemp("", "dag_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(testDir) // Clean up after test

	// Generate test data with minimal structure:
	// - 2 items max per directory (to ensure we have at least 2 files)
	// - 1 level deep
	GenerateDummyDirectory(filepath.Join(testDir, "input"), 2, 4, 1, 3)

	// Create a test DAG from the directory with timestamp to ensure root is built correctly
	dag, err := CreateDag(filepath.Join(testDir, "input"), true)
	if err != nil {
		t.Fatalf("Failed to create DAG: %v", err)
	}

	// Verify the DAG was created correctly
	err = dag.Verify()
	if err != nil {
		t.Fatalf("DAG verification failed: %v", err)
	}

	// Count file leaves to ensure we have enough for testing
	var fileCount int
	for _, leaf := range dag.Leafs {
		if leaf.Type == FileLeafType {
			fileCount++
		}
	}
	if fileCount < 1 {
		t.Fatal("No file leaves found in the DAG")
	}

	// Test getting partial DAG with the first leaf
	partial, err := dag.GetPartial(0, 1)
	if err != nil {
		t.Fatalf("Failed to get partial DAG: %v", err)
	}

	// Verify the partial DAG
	err = partial.Verify()
	if err != nil {
		t.Errorf("Partial DAG verification failed: %v", err)
	}
}

func TestGetPartialSingleChild(t *testing.T) {
	// Create a temporary test directory
	testDir, err := os.MkdirTemp("", "dag_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(testDir) // Clean up after test

	// Generate test data with a simple structure:
	// - 2 items max per directory
	// - 2 levels deep (to ensure we have a parent-child relationship)
	GenerateDummyDirectory(filepath.Join(testDir, "input"), 2, 4, 2, 5)

	// Create a test DAG from the directory with timestamp to ensure root is built correctly
	dag, err := CreateDag(filepath.Join(testDir, "input"), true)
	if err != nil {
		t.Fatalf("Failed to create DAG: %v", err)
	}

	// Verify the DAG was created correctly
	err = dag.Verify()
	if err != nil {
		t.Fatalf("DAG verification failed: %v", err)
	}

	// Count file leaves to ensure we have enough for testing
	var fileCount int
	for _, leaf := range dag.Leafs {
		if leaf.Type == FileLeafType {
			fileCount++
		}
	}
	if fileCount < 1 {
		t.Fatal("No file leaves found in the DAG")
	}

	// Test getting partial DAG with the first leaf
	partial, err := dag.GetPartial(0, 1)
	if err != nil {
		t.Fatalf("Failed to get partial DAG: %v", err)
	}

	// Verify the partial DAG
	err = partial.Verify()
	if err != nil {
		t.Errorf("Partial DAG verification failed: %v", err)
	}
}
