package dag

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLeafByLeafTransmission(t *testing.T) {
	testDir, err := os.MkdirTemp("", "dag_transmission_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	GenerateDummyDirectory(filepath.Join(testDir, "input"), 3, 5, 2, 3)

	originalDag, err := CreateDag(filepath.Join(testDir, "input"), true)
	if err != nil {
		t.Fatalf("Failed to create DAG: %v", err)
	}

	err = originalDag.Verify()
	if err != nil {
		t.Fatalf("Original DAG verification failed: %v", err)
	}

	sequence := originalDag.GetLeafSequence()

	if len(sequence) == 0 {
		t.Fatal("No transmission packets generated")
	}

	t.Logf("Generated %d transmission packets", len(sequence))

	receiverDag := &Dag{
		Root:  originalDag.Root,
		Leafs: make(map[string]*DagLeaf),
	}

	for i, p := range sequence {
		bytes, err := p.ToCBOR()
		if err != nil {
			t.Fatalf("Failed to serialize packet")
		}

		packet, err := TransmissionPacketFromCBOR(bytes)
		if err != nil {
			t.Fatalf("Failed to deserialize packet")
		}

		err = receiverDag.ApplyAndVerifyTransmissionPacket(packet)
		if err != nil {
			t.Fatalf("Packet verification failed after packet %d: %v", i, err)
		}

		t.Logf("Successfully verified packet %d, DAG now has %d leaves", i, len(receiverDag.Leafs))
	}

	if len(receiverDag.Leafs) != len(originalDag.Leafs) {
		t.Fatalf("Receiver DAG has %d leaves, expected %d",
			len(receiverDag.Leafs), len(originalDag.Leafs))
	}

	// Final full verification to ensure everything is correct
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

	t.Log("Successfully verified full DAG after discarding proofs")
}

func TestOptimizedLeafByLeafTransmission(t *testing.T) {
	testDir, err := os.MkdirTemp("", "dag_optimized_transmission_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	GenerateDummyDirectory(filepath.Join(testDir, "input"), 5, 10, 3, 6)

	t.Log("Directory Generated")

	originalDag, err := CreateDag(filepath.Join(testDir, "input"), true)
	if err != nil {
		t.Fatalf("Failed to create DAG: %v", err)
	}

	t.Log("Dag Created")

	err = originalDag.Verify()
	if err != nil {
		t.Fatalf("Original DAG verification failed: %v", err)
	}

	t.Log("Dag Verified")

	sequence := originalDag.GetLeafSequence()

	if len(sequence) == 0 {
		t.Fatal("No transmission packets generated")
	}

	t.Logf("Generated %d transmission packets for optimized test", len(sequence))

	receiverDag := &Dag{
		Root:  originalDag.Root,
		Leafs: make(map[string]*DagLeaf),
	}

	// Track performance by measuring time per packet
	for i, p := range sequence {
		start := time.Now()

		bytes, err := p.ToCBOR()
		if err != nil {
			t.Fatalf("Failed to serialize packet")
		}

		packet, err := TransmissionPacketFromCBOR(bytes)
		if err != nil {
			t.Fatalf("Failed to deserialize packet")
		}

		// Use individual packet verification - this should be O(1) per packet
		err = receiverDag.ApplyAndVerifyTransmissionPacket(packet)
		if err != nil {
			t.Fatalf("Optimized packet verification failed after packet %d: %v", i, err)
		}

		elapsed := time.Since(start)
		t.Logf("Packet %d verified in %v, DAG now has %d leaves", i, elapsed, len(receiverDag.Leafs))
	}

	if len(receiverDag.Leafs) != len(originalDag.Leafs) {
		t.Fatalf("Receiver DAG has %d leaves, expected %d",
			len(receiverDag.Leafs), len(originalDag.Leafs))
	}

	// Only do final verification once at the end
	finalStart := time.Now()
	err = receiverDag.Verify()
	finalElapsed := time.Since(finalStart)
	if err != nil {
		t.Fatalf("Final full DAG verification failed: %v", err)
	}

	t.Logf("Final full verification completed in %v", finalElapsed)
	t.Log("Optimized transmission completed successfully")
}

func TestPartialDagTransmission(t *testing.T) {
	testDir, err := os.MkdirTemp("", "dag_partial_transmission_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	GenerateDummyDirectory(filepath.Join(testDir, "input"), 3, 5, 2, 4)

	originalDag, err := CreateDag(filepath.Join(testDir, "input"), true)
	if err != nil {
		t.Fatalf("Failed to create DAG: %v", err)
	}

	err = originalDag.Verify()
	if err != nil {
		t.Fatalf("Original DAG verification failed: %v", err)
	}

	partialDag, err := originalDag.GetPartial(0, 3)
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

	receiverDag := &Dag{
		Root:  partialDag.Root,
		Leafs: make(map[string]*DagLeaf),
	}

	for i, p := range sequence {
		bytes, err := p.ToCBOR()
		if err != nil {
			t.Fatalf("Failed to serialize packet from partial DAG")
		}

		packet, err := TransmissionPacketFromCBOR(bytes)
		if err != nil {
			t.Fatalf("Failed to deserialize packet from partial DAG")
		}

		receiverDag.ApplyTransmissionPacket(packet)

		err = receiverDag.Verify()
		if err != nil {
			t.Fatalf("Verification failed after packet %d from partial DAG: %v", i, err)
		}

		t.Logf("Successfully verified after packet %d from partial DAG, DAG now has %d leaves",
			i, len(receiverDag.Leafs))
	}

	if len(receiverDag.Leafs) != len(partialDag.Leafs) {
		t.Fatalf("Receiver DAG has %d leaves, expected %d (same as partial DAG)",
			len(receiverDag.Leafs), len(partialDag.Leafs))
	}

	if !receiverDag.IsPartial() {
		t.Fatal("Reconstructed DAG not recognized as partial")
	}

	t.Log("Successfully transmitted and verified partial DAG")
}
