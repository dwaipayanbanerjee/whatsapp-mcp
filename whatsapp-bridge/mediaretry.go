package main

// Media retry recovery: WhatsApp expires media from its CDN after a few
// weeks, after which downloads fail with 403/404/410. The official client
// recovers by sending a "media retry" receipt that asks the sender's phone
// to re-upload the file; the phone answers with a fresh direct path. This
// file implements that flow so /api/download can transparently rescue
// expired media (voice notes in particular) instead of failing permanently.
//
// The recovery is synchronous from the caller's point of view: it blocks
// inside downloadMedia until the sender's phone responds or the wait times
// out. If the phone is offline or no longer has the file, the original
// download error semantics are preserved.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waMmsRetry"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// mediaRetryWaiters routes *events.MediaRetry notifications to the download
// request that solicited them, keyed by message ID.
var mediaRetryWaiters sync.Map // types.MessageID -> chan *events.MediaRetry

// mediaRetryTimeout bounds the wait for the sender's phone to re-upload.
// The phone must be online; 45s covers a push-wakeup round trip.
const mediaRetryTimeout = 45 * time.Second

// deliverMediaRetry hands a retry notification to the waiting downloader, if
// any. Called from the global event handler; never blocks.
func deliverMediaRetry(evt *events.MediaRetry) {
	if ch, ok := mediaRetryWaiters.Load(evt.MessageID); ok {
		select {
		case ch.(chan *events.MediaRetry) <- evt:
		default:
		}
	}
}

// isExpiredMediaErr reports whether err is the CDN telling us the encrypted
// blob is gone (403/404/410) — the only case a media retry receipt can fix.
func isExpiredMediaErr(err error) bool {
	return errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith403) ||
		errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith404) ||
		errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith410)
}

// recoverViaMediaRetry asks the sender's phone to re-upload expired media,
// waits for the retry notification, and downloads from the fresh direct path.
func recoverViaMediaRetryContext(ctx context.Context, client *whatsmeow.Client, messageStore *MessageStore, messageID, chatJID string, dl *MediaDownloader) ([]byte, error) {
	chat, err := types.ParseJID(chatJID)
	if err != nil {
		return nil, fmt.Errorf("parse chat jid: %w", err)
	}
	var sender string
	var isFromMe bool
	var ts time.Time
	err = messageStore.db.QueryRowContext(ctx,
		"SELECT sender, is_from_me, timestamp FROM messages WHERE id = ? AND chat_jid = ?",
		messageID, chatJID,
	).Scan(&sender, &isFromMe, &ts)
	if err != nil {
		return nil, fmt.Errorf("find message for media retry: %w", err)
	}

	info := &types.MessageInfo{
		MessageSource: types.MessageSource{
			Chat:     chat,
			IsFromMe: isFromMe,
			IsGroup:  chat.Server == types.GroupServer,
		},
		ID:        messageID,
		Timestamp: ts,
	}
	switch {
	case info.IsGroup:
		info.Sender = types.NewJID(sender, types.DefaultUserServer)
	case isFromMe:
		if client.Store != nil && client.Store.ID != nil {
			info.Sender = client.Store.ID.ToNonAD()
		}
	default:
		info.Sender = chat
	}

	ch := make(chan *events.MediaRetry, 1)
	if _, loaded := mediaRetryWaiters.LoadOrStore(messageID, ch); loaded {
		return nil, fmt.Errorf("media retry already in flight for %s", messageID)
	}
	defer mediaRetryWaiters.Delete(messageID)

	fmt.Printf("📡 Media expired for %s; asking sender's phone to re-upload...\n", messageID)
	if err := client.SendMediaRetryReceipt(ctx, info, dl.MediaKey); err != nil {
		return nil, fmt.Errorf("send media retry receipt: %w", err)
	}

	var evt *events.MediaRetry
	select {
	case evt = <-ch:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(mediaRetryTimeout):
		return nil, fmt.Errorf("media retry timed out after %s (sender's phone offline or unresponsive)", mediaRetryTimeout)
	}

	notif, err := whatsmeow.DecryptMediaRetryNotification(evt, dl.MediaKey)
	if err != nil {
		return nil, fmt.Errorf("decrypt media retry notification: %w", err)
	}
	if notif.GetResult() != waMmsRetry.MediaRetryNotification_SUCCESS {
		return nil, fmt.Errorf("media retry rejected by sender's phone: %s", notif.GetResult())
	}
	directPath := notif.GetDirectPath()
	if directPath == "" {
		return nil, fmt.Errorf("media retry succeeded but returned no direct path")
	}

	data, err := client.DownloadMediaWithPath(ctx, directPath, dl.FileEncSHA256, dl.FileSHA256, dl.MediaKey, dl.MediaType, "", false)
	if err != nil {
		return nil, fmt.Errorf("download after media retry: %w", err)
	}
	fmt.Printf("📡 Media retry recovered %d bytes for %s\n", len(data), messageID)
	return data, nil
}
