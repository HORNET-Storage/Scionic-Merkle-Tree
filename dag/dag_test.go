package dag

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFull(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test")
	if err != nil {
		t.Fatalf("Could not create temp directory: %s", err)
	}

	defer os.RemoveAll(tmpDir)

	GenerateDummyDirectory(filepath.Join(tmpDir, "input"), 3, 6, 1, 3)
	if err != nil {
		t.Fatalf("Could not generate dummy directory: %s", err)
	}

	input := filepath.Join(tmpDir, "input")
	output := filepath.Join(tmpDir, "output")

	SetChunkSize(4096)

	dag, err := CreateDag(input, true)
	if err != nil {
		t.Fatalf("Error: %s", err)
	}

	err = dag.Verify()
	if err != nil {
		t.Fatalf("Error: %s", err)
	}

	err = dag.CreateDirectory(output)
	if err != nil {
		t.Fatalf("Error: %s", err)
	}
}
