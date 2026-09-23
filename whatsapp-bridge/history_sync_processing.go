package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

const historyQueueSize = 32
const historyChunkTimeout = 5 * time.Minute

type historySyncJob struct {
	notification *waE2E.HistorySyncNotification
	sessionID    string
	chunkKey     string
	tracked      bool
}

// One worker keeps history storage and attachment capture off the event loop.
// Media retry responses arrive on that same loop, so neither processing nor a
// full queue may block it.
type historySyncProcessor struct {
	ctx          context.Context
	cancel       context.CancelFunc
	done         chan struct{}
	closeOnce    sync.Once
	enqueueMu    sync.Mutex
	queue        chan historySyncJob
	tracker      *onDemandTracker
	logger       waLog.Logger
	download     func(context.Context, *waE2E.HistorySyncNotification) (*waHistorySync.HistorySync, error)
	store        func(*waHistorySync.HistorySync) historySyncResult
	captureMedia func(context.Context, historyMediaDownload) error
	deleteMedia  func(context.Context, *waE2E.HistorySyncNotification) error
}

func newHistorySyncProcessor(client *whatsmeow.Client, messageStore *MessageStore, tracker *onDemandTracker, logger waLog.Logger) *historySyncProcessor {
	p := &historySyncProcessor{
		tracker: tracker,
		logger:  logger,
		download: func(ctx context.Context, notification *waE2E.HistorySyncNotification) (*waHistorySync.HistorySync, error) {
			// Populate PN/LID mappings before the bridge resolves conversation IDs.
			return client.DownloadHistorySync(ctx, notification, true)
		},
		store: func(data *waHistorySync.HistorySync) historySyncResult {
			return handleHistorySync(client, messageStore, &events.HistorySync{Data: data}, logger)
		},
		captureMedia: func(ctx context.Context, task historyMediaDownload) error {
			_, _, _, _, err := downloadMediaWithContext(ctx, client, messageStore, task.MessageID, task.ChatJID)
			return err
		},
		deleteMedia: func(ctx context.Context, notification *waE2E.HistorySyncNotification) error {
			// Inline history has no CDN object to delete.
			if notification.GetDirectPath() == "" {
				return nil
			}
			return client.DeleteMedia(ctx, whatsmeow.MediaHistory, notification.GetDirectPath(), notification.GetFileEncSHA256(), notification.GetEncHandle())
		},
	}
	p.start()
	return p
}

func (p *historySyncProcessor) start() {
	p.ctx, p.cancel = context.WithCancel(context.Background())
	p.done = make(chan struct{})
	p.queue = make(chan historySyncJob, historyQueueSize)
	go p.run()
}

func (p *historySyncProcessor) close() {
	p.closeOnce.Do(func() {
		p.enqueueMu.Lock()
		p.cancel()
		p.enqueueMu.Unlock()
	})
	<-p.done
}

func historyNotificationKey(messageID string, notification *waE2E.HistorySyncNotification) string {
	if hash := notification.GetFileEncSHA256(); len(hash) > 0 {
		return "hash:" + hex.EncodeToString(hash)
	}
	if messageID != "" {
		return "message:" + messageID
	}
	// Inline payloads may lack a media hash. A deterministic key also prevents
	// malformed notifications with no ID from bypassing duplicate detection.
	encoded, _ := proto.MarshalOptions{Deterministic: true}.Marshal(notification)
	hash := sha256.Sum256(encoded)
	return "notification:" + hex.EncodeToString(hash[:])
}

// enqueue reports whether the event was a history notification, including a
// rejected one, so it can never fall through to the ordinary message handler.
func (p *historySyncProcessor) enqueue(event *events.Message) bool {
	if event == nil {
		return false
	}
	raw := event.RawMessage
	if wrapped := raw.GetDeviceSentMessage().GetMessage(); wrapped != nil {
		raw = wrapped
	}
	notification := raw.GetProtocolMessage().GetHistorySyncNotification()
	if notification == nil {
		return false
	}
	// whatsmeow rejects non-self protocol side effects, but still dispatches
	// their raw Message event. Preserve that same trust boundary here.
	if !event.Info.IsFromMe {
		p.logger.Warnf("Rejected history notification from another account")
		return true
	}
	p.enqueueMu.Lock()
	defer p.enqueueMu.Unlock()
	if p.ctx.Err() != nil {
		return true
	}
	job := historySyncJob{
		notification: notification,
		chunkKey:     historyNotificationKey(event.Info.ID, notification),
	}
	if notification.GetSyncType() == waE2E.HistorySyncType_ON_DEMAND {
		job.sessionID = notification.GetPeerDataRequestSessionID()
	}
	matched, accepted := p.tracker.beginChunk(job.sessionID, job.chunkKey, time.Now().UnixMilli())
	if matched && !accepted {
		return true
	}
	job.tracked = accepted
	select {
	case p.queue <- job:
	default:
		if job.tracked {
			p.tracker.failChunk(job.sessionID, job.chunkKey, time.Now().UnixMilli())
		}
		p.logger.Errorf("History queue full: chunk not imported; history recovery must be retried")
	}
	return true
}

func (p *historySyncProcessor) run() {
	defer close(p.done)
	defer func() {
		// close holds enqueueMu while canceling, and enqueue rejects canceled
		// work under the same lock, so no new accepted job can miss this drain.
		for {
			select {
			case job := <-p.queue:
				if job.tracked {
					p.tracker.failChunk(job.sessionID, job.chunkKey, time.Now().UnixMilli())
				}
			default:
				return
			}
		}
	}()
	for {
		select {
		case <-p.ctx.Done():
			return
		case job := <-p.queue:
			if p.ctx.Err() != nil {
				if job.tracked {
					p.tracker.failChunk(job.sessionID, job.chunkKey, time.Now().UnixMilli())
				}
				return
			}
			p.process(job)
		}
	}
}

func historyChunkProgress(notification *waE2E.HistorySyncNotification, data *waHistorySync.HistorySync) *uint32 {
	// Absent progress is unknown. In particular, protobuf's default zero is
	// not evidence that a one-chunk response has finished.
	if data.Progress != nil && notification.Progress != nil && data.GetProgress() != notification.GetProgress() {
		return nil
	}
	if data.Progress != nil {
		return data.Progress
	}
	return notification.Progress
}

func (p *historySyncProcessor) process(job historySyncJob) {
	ctx, cancel := context.WithTimeout(p.ctx, historyChunkTimeout)
	defer cancel()
	data, err := p.download(ctx, job.notification)
	if err != nil || data == nil {
		if job.tracked {
			p.tracker.failChunk(job.sessionID, job.chunkKey, time.Now().UnixMilli())
		}
		p.logger.Warnf("History chunk download failed: %v", err)
		return
	}
	result := p.store(data)
	if data.Progress != nil && job.notification.Progress != nil && data.GetProgress() != job.notification.GetProgress() {
		result.storageErrors++
	}
	mediaFailed := 0
	for i, task := range result.mediaTasks {
		if ctx.Err() != nil {
			mediaFailed += len(result.mediaTasks) - i
			break
		}
		if err := p.captureMedia(ctx, task); err != nil {
			mediaFailed++
			p.logger.Warnf("History attachment capture failed: %v", err)
		}
	}
	if ctx.Err() != nil || (job.tracked && data.GetSyncType() != waHistorySync.HistorySync_ON_DEMAND) {
		result.storageErrors++
	}
	progress := historyChunkProgress(job.notification, data)
	if job.tracked {
		p.tracker.finishChunk(job.sessionID, job.chunkKey, progress, result.storedCount, result.storageErrors, mediaFailed, result.messages, time.Now().UnixMilli())
	}
	progressValue := uint32(0)
	if progress != nil {
		progressValue = *progress
	}
	p.logger.Infof("History chunk processed: stored=%d storage_errors=%d media_failed=%d correlated=%t explicit_progress=%t progress=%d", result.storedCount, result.storageErrors, mediaFailed, job.tracked, progress != nil, progressValue)
	// Keep failed chunks available for a later retry. Success includes media
	// capture, not merely committing the message metadata.
	if result.storageErrors == 0 && mediaFailed == 0 {
		if err := p.deleteMedia(ctx, job.notification); err != nil {
			p.logger.Warnf("Failed to delete processed history blob: %v", err)
		}
	}
}
