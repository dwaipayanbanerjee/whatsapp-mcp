package main

import (
	"io"
	"os"
	"time"

	waLog "go.mau.fi/whatsmeow/util/log"
)

// rotateIfNeeded implements copytruncate rotation: when logPath exceeds
// maxBytes, its contents are copied to logPath+".1" (replacing any previous
// generation) and logPath is truncated to zero. Copytruncate is required
// because launchd holds an O_APPEND fd on the log for the process lifetime —
// a rename would divert nothing. A missing log file is a no-op, not an error.
func rotateIfNeeded(logPath string, maxBytes int64) (bool, error) {
	info, err := os.Stat(logPath)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if info.Size() <= maxBytes {
		return false, nil
	}
	src, err := os.Open(logPath)
	if err != nil {
		return false, err
	}
	defer func() { _ = src.Close() }()
	dst, err := os.OpenFile(logPath+".1", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return false, err
	}
	if _, err := io.Copy(dst, src); err != nil {
		_ = dst.Close()
		return false, err
	}
	if err := dst.Close(); err != nil {
		return false, err
	}
	if err := os.Truncate(logPath, 0); err != nil {
		return false, err
	}
	return true, nil
}

// startLogRotation checks the log size on the given interval, forever.
func startLogRotation(logPath string, maxBytes int64, interval time.Duration, logger waLog.Logger) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			rotated, err := rotateIfNeeded(logPath, maxBytes)
			if err != nil {
				logger.Warnf("Log rotation failed: %v", err)
			} else if rotated {
				logger.Infof("Rotated %s (cap %d bytes)", logPath, maxBytes)
			}
		}
	}()
}
