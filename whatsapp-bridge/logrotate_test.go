package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestRotateIfNeededRotatesOversizedFile(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "bridge.log")
	content := bytes.Repeat([]byte("x"), 2048)
	if err := os.WriteFile(logPath, content, 0o644); err != nil {
		t.Fatal(err)
	}

	rotated, err := rotateIfNeeded(logPath, 1024)
	if err != nil {
		t.Fatalf("rotateIfNeeded: %v", err)
	}
	if !rotated {
		t.Fatal("expected rotation")
	}
	prev, err := os.ReadFile(logPath + ".1")
	if err != nil {
		t.Fatalf("reading rotated file: %v", err)
	}
	if !bytes.Equal(prev, content) {
		t.Fatalf("rotated file does not match original: %d vs %d bytes", len(prev), len(content))
	}
	info, err := os.Stat(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Fatalf("expected truncated log, got %d bytes", info.Size())
	}
}

func TestRotateIfNeededLeavesSmallFileAlone(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "bridge.log")
	if err := os.WriteFile(logPath, []byte("small"), 0o644); err != nil {
		t.Fatal(err)
	}
	rotated, err := rotateIfNeeded(logPath, 1024)
	if err != nil || rotated {
		t.Fatalf("expected no rotation and no error, got rotated=%v err=%v", rotated, err)
	}
	if _, err := os.Stat(logPath + ".1"); !os.IsNotExist(err) {
		t.Fatal("no .1 generation should exist")
	}
}

func TestRotateIfNeededMissingFileIsNoop(t *testing.T) {
	rotated, err := rotateIfNeeded(filepath.Join(t.TempDir(), "absent.log"), 1024)
	if err != nil || rotated {
		t.Fatalf("missing file must be a no-op, got rotated=%v err=%v", rotated, err)
	}
}

func TestRotateIfNeededOverwritesPreviousGeneration(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "bridge.log")
	if err := os.WriteFile(logPath+".1", []byte("old generation"), 0o644); err != nil {
		t.Fatal(err)
	}
	content := bytes.Repeat([]byte("y"), 2048)
	if err := os.WriteFile(logPath, content, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := rotateIfNeeded(logPath, 1024); err != nil {
		t.Fatal(err)
	}
	prev, _ := os.ReadFile(logPath + ".1")
	if !bytes.Equal(prev, content) {
		t.Fatal("previous generation was not overwritten")
	}
}
