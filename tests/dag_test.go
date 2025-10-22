package tests

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/HORNET-Storage/Scionic-Merkle-Tree/dag"
	"github.com/HORNET-Storage/Scionic-Merkle-Tree/testutil"
)

// TestFull tests the complete DAG workflow: create, verify, and recreate directory
// Tests against all fixtures to ensure the workflow works for all DAG types
func TestFull(t *testing.T) {
	testutil.RunTestWithAllFixtures(t, func(t *testing.T, d *dag.Dag, fixture testutil.TestFixture, fixturePath string) {
		// DAG is already created by the helper, verify it
		err := d.Verify()
		if err != nil {
			t.Fatalf("Verification failed for fixture %s: %v", fixture.Name, err)
		}

		// Test recreating the directory from the DAG
		tmpDir, err := os.MkdirTemp("", "dag_output_*")
		if err != nil {
			t.Fatalf("Could not create temp directory: %v", err)
		}
		defer os.RemoveAll(tmpDir)

		// Use the same directory name as the original fixture for consistent hashing
		output := filepath.Join(tmpDir, fixture.Name)
		err = d.CreateDirectory(output)
		if err != nil {
			t.Fatalf("Failed to create directory from DAG for fixture %s: %v", fixture.Name, err)
		}

		// Verify the recreated directory produces the same DAG
		recreatedDag, err := dag.CreateDag(output, false)
		if err != nil {
			t.Fatalf("Failed to create DAG from recreated directory for fixture %s: %v", fixture.Name, err)
		}

		// Verify the DAGs match
		if recreatedDag.Root != d.Root {
			t.Errorf("Recreated DAG root mismatch for fixture %s: original=%s, recreated=%s",
				fixture.Name, d.Root, recreatedDag.Root)
		}

		// Verify both DAGs pass validation
		err = recreatedDag.Verify()
		if err != nil {
			t.Fatalf("Recreated DAG verification failed for fixture %s: %v", fixture.Name, err)
		}

		t.Logf("✓ Fixture %s: Full workflow successful", fixture.Name)
	})
}
