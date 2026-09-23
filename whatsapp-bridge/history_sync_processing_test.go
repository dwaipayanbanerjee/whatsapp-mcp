package main

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func testHistoryNotification(session, messageID string, progress *uint32) *events.Message {
	return &events.Message{
		Info: types.MessageInfo{MessageSource: types.MessageSource{IsFromMe: true}, ID: messageID},
		RawMessage: &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{
			HistorySyncNotification: &waE2E.HistorySyncNotification{
				SyncType:                 waE2E.HistorySyncType_ON_DEMAND.Enum(),
				PeerDataRequestSessionID: proto.String(session),
				Progress:                 progress,
			},
		}},
	}
}

func testHistoryProcessor(t *testing.T, tracker *onDemandTracker) *historySyncProcessor {
	t.Helper()
	p := &historySyncProcessor{
		tracker: tracker, logger: testLogger(),
		download: func(_ context.Context, n *waE2E.HistorySyncNotification) (*waHistorySync.HistorySync, error) {
			return &waHistorySync.HistorySync{SyncType: waHistorySync.HistorySync_ON_DEMAND.Enum(), Progress: n.Progress}, nil
		},
		store:        func(*waHistorySync.HistorySync) historySyncResult { return historySyncResult{storedCount: 1} },
		captureMedia: func(context.Context, historyMediaDownload) error { return nil },
		deleteMedia:  func(context.Context, *waE2E.HistorySyncNotification) error { return nil },
	}
	p.start()
	t.Cleanup(p.close)
	return p
}

func reserveTestHistory(tracker *onDemandTracker) {
	tracker.reserve("request", phonePN.String(), time.Now().UnixMilli(), 60_000)
}

func waitHistoryState(t *testing.T, tracker *onDemandTracker, state string) historySyncStatus {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		status := tracker.snapshot("request", time.Now().UnixMilli())
		if status.State == state {
			return status
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("history state did not reach %s: %+v", state, tracker.snapshot("request", time.Now().UnixMilli()))
	return historySyncStatus{}
}

func TestHistoryProcessorUsesRawOwnDeviceNotification(t *testing.T) {
	tracker := &onDemandTracker{}
	reserveTestHistory(tracker)
	p := testHistoryProcessor(t, tracker)
	if p.enqueue(&events.Message{Message: &waE2E.Message{Conversation: proto.String("ordinary")}}) {
		t.Fatal("ordinary message was consumed")
	}
	event := testHistoryNotification("request", "chunk", proto.Uint32(100))
	event.Message, event.RawMessage = event.RawMessage, nil
	if p.enqueue(event) {
		t.Fatal("unwrapped-only notification was trusted")
	}
	event.RawMessage = event.Message
	event.Info.IsFromMe = false
	if !p.enqueue(event) {
		t.Fatal("untrusted protocol message fell through to regular handler")
	}
	if got := tracker.snapshot("request", time.Now().UnixMilli()); got.ChunksReceived != 0 {
		t.Fatalf("untrusted notification changed request state: %+v", got)
	}
	event.Info.IsFromMe = true
	event.RawMessage = &waE2E.Message{DeviceSentMessage: &waE2E.DeviceSentMessage{Message: event.RawMessage}}
	if !p.enqueue(event) {
		t.Fatal("own-device raw notification was ignored")
	}
	waitHistoryState(t, tracker, "completed")
}

func TestHistoryProcessorWaitsForAllNotifiedChunksAndMedia(t *testing.T) {
	tracker := &onDemandTracker{}
	reserveTestHistory(tracker)
	p := testHistoryProcessor(t, tracker)
	mediaStarted, releaseMedia := make(chan struct{}), make(chan struct{})
	var captures atomic.Int32
	p.store = func(*waHistorySync.HistorySync) historySyncResult {
		return historySyncResult{storedCount: 1, mediaTasks: []historyMediaDownload{{MessageID: "media"}}}
	}
	p.captureMedia = func(ctx context.Context, _ historyMediaDownload) error {
		if captures.Add(1) == 1 {
			close(mediaStarted)
			select {
			case <-releaseMedia:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	// The terminal notification may be processed before an already-notified
	// earlier chunk; its progress alone cannot bypass pending storage/media.
	p.enqueue(testHistoryNotification("request", "terminal", proto.Uint32(100)))
	<-mediaStarted
	p.enqueue(testHistoryNotification("request", "earlier", proto.Uint32(50)))
	p.enqueue(testHistoryNotification("request", "terminal", proto.Uint32(100)))
	if got := tracker.snapshot("request", time.Now().UnixMilli()); got.State != "receiving" || got.PendingChunks != 2 || got.CompletedAtMs != 0 {
		t.Fatalf("premature completion or duplicate chunk: %+v", got)
	}
	close(releaseMedia)
	got := waitHistoryState(t, tracker, "completed")
	if got.StoredCount != 2 || got.ChunksReceived != 2 || captures.Load() != 2 {
		t.Fatalf("chunks/media were not completed exactly once: status=%+v captures=%d", got, captures.Load())
	}
}

func TestHistoryProcessorDoesNotAttributeUnknownSessionOrMissingProgress(t *testing.T) {
	tracker := &onDemandTracker{}
	reserveTestHistory(tracker)
	p := testHistoryProcessor(t, tracker)
	stored := make(chan struct{}, 2)
	p.store = func(*waHistorySync.HistorySync) historySyncResult {
		stored <- struct{}{}
		return historySyncResult{storedCount: 1}
	}
	p.enqueue(testHistoryNotification("unrelated", "other", proto.Uint32(100)))
	<-stored
	if got := tracker.snapshot("request", time.Now().UnixMilli()); got.State != "pending" || got.StoredCount != 0 {
		t.Fatalf("unrelated history satisfied active request: %+v", got)
	}
	p.enqueue(testHistoryNotification("request", "unknown-progress", nil))
	<-stored
	p.close()
	if got := tracker.snapshot("request", time.Now().UnixMilli()); got.CompletedAtMs != 0 || got.Completion != "" {
		t.Fatalf("missing terminal evidence completed request: %+v", got)
	}
}

func TestHistoryProcessorFailuresNeverCompleteOrDeleteBlob(t *testing.T) {
	for _, kind := range []string{"download", "storage", "media", "incomplete-media"} {
		t.Run(kind, func(t *testing.T) {
			tracker := &onDemandTracker{}
			reserveTestHistory(tracker)
			p := testHistoryProcessor(t, tracker)
			var deleted atomic.Bool
			p.deleteMedia = func(context.Context, *waE2E.HistorySyncNotification) error { deleted.Store(true); return nil }
			switch kind {
			case "download":
				p.download = func(context.Context, *waE2E.HistorySyncNotification) (*waHistorySync.HistorySync, error) {
					return nil, errors.New("invalid history blob")
				}
			case "storage":
				ms := newTestMessageStore(t)
				ms.db.Close()
				p.store = func(*waHistorySync.HistorySync) historySyncResult {
					return handleHistorySync(newTestClient(&mockLIDStore{}), ms, testHistoryRows(), testLogger())
				}
			case "media":
				p.store = func(*waHistorySync.HistorySync) historySyncResult {
					return historySyncResult{storedCount: 1, mediaTasks: []historyMediaDownload{{MessageID: "media"}}}
				}
				p.captureMedia = func(context.Context, historyMediaDownload) error { return errors.New("media unavailable") }
			case "incomplete-media":
				t.Chdir(t.TempDir())
				ms := newTestMessageStore(t)
				history := testHistoryRows()
				history.Data.Conversations[0].Messages[0].Message.Message = &waProto.Message{ImageMessage: &waProto.ImageMessage{Caption: proto.String("image fixture")}}
				p.store = func(*waHistorySync.HistorySync) historySyncResult {
					return handleHistorySync(newTestClient(&mockLIDStore{}), ms, history, testLogger())
				}
				p.captureMedia = func(ctx context.Context, task historyMediaDownload) error {
					_, _, _, _, err := downloadMediaWithContext(ctx, nil, ms, task.MessageID, task.ChatJID)
					return err
				}
			}
			p.enqueue(testHistoryNotification("request", "chunk", proto.Uint32(100)))
			got := waitHistoryState(t, tracker, "failed")
			p.close()
			if got.CompletedAtMs != 0 || deleted.Load() {
				t.Fatalf("failed request reported complete or deleted retryable blob: %+v", got)
			}
		})
	}
}

func TestHistoryProcessorQueueOverflowAndCancellationAreBounded(t *testing.T) {
	tracker := &onDemandTracker{}
	reserveTestHistory(tracker)
	p := testHistoryProcessor(t, tracker)
	started := make(chan struct{})
	p.captureMedia = func(ctx context.Context, _ historyMediaDownload) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}
	p.store = func(*waHistorySync.HistorySync) historySyncResult {
		return historySyncResult{mediaTasks: []historyMediaDownload{{MessageID: "media"}}}
	}
	p.enqueue(testHistoryNotification("request", "first", proto.Uint32(100)))
	<-started
	for i := 0; i <= historyQueueSize; i++ {
		p.enqueue(testHistoryNotification("request", fmt.Sprintf("queued-%d", i), proto.Uint32(50)))
	}
	got := tracker.snapshot("request", time.Now().UnixMilli())
	if got.State != "failed" || got.DownloadErrors != 1 {
		t.Fatalf("overflow was not surfaced: %+v", got)
	}
	closed := make(chan struct{})
	go func() { p.close(); close(closed) }()
	select {
	case <-closed:
		if status := tracker.snapshot("request", time.Now().UnixMilli()); status.PendingChunks != 0 || status.InFlight {
			t.Fatalf("shutdown abandoned accepted chunks: %+v", status)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not cancel media work")
	}
}

func TestHistoryChunkProgressRequiresConsistentExplicitEvidence(t *testing.T) {
	for _, test := range []struct {
		notif, blob *uint32
		terminal    bool
	}{
		{nil, nil, false}, {proto.Uint32(100), nil, true}, {nil, proto.Uint32(100), true},
		{proto.Uint32(100), proto.Uint32(50), false}, {proto.Uint32(0), proto.Uint32(0), false},
	} {
		got := historyChunkProgress(&waE2E.HistorySyncNotification{Progress: test.notif}, &waHistorySync.HistorySync{Progress: test.blob})
		if (got != nil && *got == 100) != test.terminal {
			t.Fatalf("unexpected terminal evidence: %v", got)
		}
	}
}

func testHistoryRows() *events.HistorySync {
	return &events.HistorySync{Data: &waProto.HistorySync{
		SyncType: waProto.HistorySync_ON_DEMAND.Enum(), Progress: proto.Uint32(100),
		Conversations: []*waProto.Conversation{{ID: proto.String(phonePN.String()), Messages: []*waProto.HistorySyncMsg{{Message: &waProto.WebMessageInfo{
			Key:              &waCommon.MessageKey{ID: proto.String("history-row"), FromMe: proto.Bool(false)},
			MessageTimestamp: proto.Uint64(1700000000), Message: &waProto.Message{Conversation: proto.String("history fixture")},
		}}}}},
	}}
}

func TestHistoryProcessorDownloadsInlinePayloadAndReturnsStoredReferences(t *testing.T) {
	client := newTestClient(&mockLIDStore{})
	client.Log = testLogger()
	ms := newTestMessageStore(t)
	tracker := &onDemandTracker{}
	reserveTestHistory(tracker)
	p := newHistorySyncProcessor(client, ms, tracker, testLogger())
	defer p.close()
	data, err := proto.Marshal(testHistoryRows().Data)
	if err != nil {
		t.Fatal(err)
	}
	var compressed bytes.Buffer
	writer := zlib.NewWriter(&compressed)
	if _, err := writer.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	event := testHistoryNotification("request", "inline", nil)
	event.RawMessage.ProtocolMessage.HistorySyncNotification.InitialHistBootstrapInlinePayload = compressed.Bytes()
	p.enqueue(event)
	status := waitHistoryState(t, tracker, "completed")
	if len(status.Messages) != 1 || status.Messages[0].MessageID != "history-row" || status.Messages[0].ChatJID != phonePN.String() {
		t.Fatalf("missing stored references: %+v", status.Messages)
	}
	var count int
	if err := ms.db.QueryRow("SELECT count(*) FROM messages WHERE id = 'history-row'").Scan(&count); err != nil || count != 1 {
		t.Fatalf("inline payload was not stored: count=%d err=%v", count, err)
	}
}

func TestHistoryStorageKeepsValidRowsAfterMalformedFirstRow(t *testing.T) {
	history := testHistoryRows()
	history.Data.Conversations[0].Messages = append([]*waProto.HistorySyncMsg{nil}, history.Data.Conversations[0].Messages...)
	ms := newTestMessageStore(t)
	result := handleHistorySync(newTestClient(&mockLIDStore{}), ms, history, testLogger())
	if result.storedCount != 1 || result.storageErrors != 1 {
		t.Fatalf("valid row hidden by malformed first row: %+v", result)
	}
	if _, err := ms.db.Exec("CREATE TRIGGER reject_history BEFORE INSERT ON messages BEGIN SELECT RAISE(ABORT, 'fixture failure'); END"); err != nil {
		t.Fatal(err)
	}
	result = handleHistorySync(newTestClient(&mockLIDStore{}), ms, history, testLogger())
	if result.storageErrors != 2 || result.storedCount != 0 || len(result.messages) != 0 {
		t.Fatalf("failed row counted as persisted: %+v", result)
	}
}

func TestDocumentMediaCachedDownloadRecordsActualFilename(t *testing.T) {
	t.Chdir(t.TempDir())
	ms := newTestMessageStore(t)
	chat := phonePN.String()
	ts := time.Unix(1700000000, 0)
	data := []byte("document fixture bytes")
	hash := sha256.Sum256(data)
	if err := ms.StoreChat(chat, "fixture", ts); err != nil {
		t.Fatal(err)
	}
	if err := ms.StoreMessage("document-id", chat, phonePN.User, "", ts, false, "document", "original.pdf", "", nil, hash[:], nil, uint64(len(data)), ""); err != nil {
		t.Fatal(err)
	}
	filename := "document_" + ts.Format("20060102_150405") + "_document-id"
	path := filepath.Join("store", chat, filename)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := writeMediaFile(path, data); err != nil {
		t.Fatal(err)
	}
	success, _, actual, _, err := downloadMedia(nil, ms, "document-id", chat)
	if err != nil || !success || actual != filename {
		t.Fatalf("cached document download: success=%t name=%q err=%v", success, actual, err)
	}
	var stored string
	if err := ms.db.QueryRow("SELECT filename FROM messages WHERE id = 'document-id'").Scan(&stored); err != nil || stored != filename {
		t.Fatalf("archive will read wrong filename: %q err=%v", stored, err)
	}
	if _, err := ms.db.Exec("CREATE TRIGGER reject_filename BEFORE UPDATE ON messages BEGIN SELECT RAISE(ABORT, 'fixture failure'); END"); err != nil {
		t.Fatal(err)
	}
	if success, _, _, _, err := downloadMedia(nil, ms, "document-id", chat); success || err == nil {
		t.Fatal("failed filename persistence was reported successful")
	}
}

func TestCachedMediaMustMatchExpectedBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "media")
	data := []byte("complete data")
	hash := sha256.Sum256(data)
	if err := writeMediaFile(path, data); err != nil {
		t.Fatal(err)
	}
	if !verifiedMediaFile(path, hash[:], uint64(len(data))) {
		t.Fatal("valid media rejected")
	}
	if err := os.WriteFile(path, []byte("wrong bytes!!"), 0644); err != nil {
		t.Fatal(err)
	}
	if verifiedMediaFile(path, hash[:], uint64(len(data))) {
		t.Fatal("corrupt media counted as captured")
	}
	if err := os.WriteFile(path, data[:3], 0644); err != nil {
		t.Fatal(err)
	}
	if verifiedMediaFile(path, hash[:], uint64(len(data))) {
		t.Fatal("partial media counted as captured")
	}
}

func TestMediaCaptureCanceledBeforeDatabaseRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ms := newTestMessageStore(t)
	if success, _, _, _, err := downloadMediaWithContext(ctx, nil, ms, "missing", phonePN.String()); success || err == nil {
		t.Fatal("canceled media call continued")
	}
	if _, err := recoverViaMediaRetryContext(ctx, nil, ms, "missing", phonePN.String(), &MediaDownloader{}); err == nil {
		t.Fatal("canceled retry call continued")
	}
}

func TestHistoryProcessorProgressConflictCannotReuseEarlierTerminal(t *testing.T) {
	tracker := &onDemandTracker{}
	reserveTestHistory(tracker)
	p := testHistoryProcessor(t, tracker)
	started, release := make(chan struct{}), make(chan struct{})
	p.download = func(ctx context.Context, notification *waE2E.HistorySyncNotification) (*waHistorySync.HistorySync, error) {
		if notification.GetProgress() == 100 {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return &waHistorySync.HistorySync{SyncType: waHistorySync.HistorySync_ON_DEMAND.Enum(), Progress: proto.Uint32(100)}, nil
		}
		return &waHistorySync.HistorySync{SyncType: waHistorySync.HistorySync_ON_DEMAND.Enum(), Progress: proto.Uint32(99)}, nil
	}
	p.enqueue(testHistoryNotification("request", "terminal", proto.Uint32(100)))
	<-started
	p.enqueue(testHistoryNotification("request", "conflict", proto.Uint32(50)))
	close(release)
	got := waitHistoryState(t, tracker, "failed")
	if got.StorageErrors != 1 || got.CompletedAtMs != 0 {
		t.Fatalf("conflicting progress reused terminal evidence: %+v", got)
	}
}

func TestHistoryStorageRejectsMalformedSupportedRows(t *testing.T) {
	for _, kind := range []string{"missing-chat", "invalid-chat", "missing-id", "missing-timestamp"} {
		t.Run(kind, func(t *testing.T) {
			history := testHistoryRows()
			conversation := history.Data.Conversations[0]
			switch kind {
			case "missing-chat":
				conversation.ID = nil
			case "invalid-chat":
				conversation.ID = proto.String("not-a-jid")
			case "missing-id":
				conversation.Messages[0].Message.Key.ID = nil
			case "missing-timestamp":
				conversation.Messages[0].Message.MessageTimestamp = nil
			}
			result := handleHistorySync(newTestClient(&mockLIDStore{}), newTestMessageStore(t), history, testLogger())
			if result.storageErrors == 0 || result.storedCount != 0 || len(result.messages) != 0 {
				t.Fatalf("malformed supported row was silently lost: %+v", result)
			}
		})
	}
}

func TestSparseHistoryPreservesRecoverableMediaMetadata(t *testing.T) {
	t.Chdir(t.TempDir())
	ms := newTestMessageStore(t)
	chat := phonePN.String()
	ts := time.Unix(1700000000, 0)
	data := []byte("cached image fixture")
	hash := sha256.Sum256(data)
	if err := ms.StoreChat(chat, "fixture", ts); err != nil {
		t.Fatal(err)
	}
	filename := "image_" + ts.Format("20060102_150405") + "_history-row.jpg"
	if err := ms.StoreMessage("history-row", chat, phonePN.User, "", ts, false, "image", filename, "https://example.invalid/media", []byte{1}, hash[:], []byte{3}, uint64(len(data)), ""); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join("store", chat, filename)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := writeMediaFile(path, data); err != nil {
		t.Fatal(err)
	}
	history := testHistoryRows()
	history.Data.Conversations[0].Messages[0].Message.Message = &waProto.Message{ImageMessage: &waProto.ImageMessage{Caption: proto.String("history caption")}}
	result := handleHistorySync(newTestClient(&mockLIDStore{}), ms, history, testLogger())
	if len(result.mediaTasks) != 1 || result.storageErrors != 0 {
		t.Fatalf("sparse media was not scheduled: %+v", result)
	}
	if success, _, _, _, err := downloadMedia(nil, ms, "history-row", chat); !success || err != nil {
		t.Fatalf("sparse history destroyed cached media metadata: %v", err)
	}
	var url string
	var mediaKey, encHash []byte
	if err := ms.db.QueryRow("SELECT url, media_key, file_enc_sha256 FROM messages WHERE id = 'history-row'").Scan(&url, &mediaKey, &encHash); err != nil {
		t.Fatal(err)
	}
	if url == "" || !bytes.Equal(mediaKey, []byte{1}) || !bytes.Equal(encHash, []byte{3}) {
		t.Fatal("sparse history cleared recovery metadata")
	}
}
