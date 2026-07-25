package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

const onDemandInflightTTLMs = 60_000

type onDemandCompletion struct {
	RequestID     string
	ChatJID       string
	RequestedAtMs int64
	CompletedAtMs int64
	StoredCount   int
}

type onDemandInflight struct {
	RequestID     string
	ChatJID       string
	RequestedAtMs int64
	ExpiresAtMs   int64
}

// onDemandTracker serializes on-demand history requests and attributes each
// ON_DEMAND completion to the request that caused it. The WhatsApp protocol
// does not echo request ids inside history chunks, so allowing only one
// request in flight at a time is what makes the attribution sound.
type onDemandTracker struct {
	mu       sync.Mutex
	inflight *onDemandInflight
	last     onDemandCompletion
}

// reserve returns nil and records the reservation when the slot is free (or
// the current occupant has expired); otherwise it returns a copy of the
// blocking in-flight record.
func (t *onDemandTracker) reserve(chatJID string, nowMs int64, ttlMs int64) *onDemandInflight {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.inflight != nil && nowMs <= t.inflight.ExpiresAtMs {
		blocking := *t.inflight
		return &blocking
	}
	t.inflight = &onDemandInflight{
		ChatJID:       chatJID,
		RequestedAtMs: nowMs,
		ExpiresAtMs:   nowMs + ttlMs,
	}
	return nil
}

// commit stamps the whatsmeow request id onto the current reservation.
func (t *onDemandTracker) commit(requestID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.inflight != nil {
		t.inflight.RequestID = requestID
	}
}

// release drops the reservation (the peer send failed).
func (t *onDemandTracker) release() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.inflight = nil
}

// complete consumes any reservation into the last-completion record; a
// completion with no reservation is recorded with empty attribution.
func (t *onDemandTracker) complete(nowMs int64, storedCount int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	record := onDemandCompletion{CompletedAtMs: nowMs, StoredCount: storedCount}
	if t.inflight != nil {
		record.RequestID = t.inflight.RequestID
		record.ChatJID = t.inflight.ChatJID
		record.RequestedAtMs = t.inflight.RequestedAtMs
	}
	t.inflight = nil
	t.last = record
}

// snapshot returns the last completion and the current unexpired in-flight
// record (nil if none).
func (t *onDemandTracker) snapshot(nowMs int64) (onDemandCompletion, *onDemandInflight) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.inflight == nil || nowMs > t.inflight.ExpiresAtMs {
		return t.last, nil
	}
	inflight := *t.inflight
	return t.last, &inflight
}

var onDemandHistory = &onDemandTracker{}

// markOnDemandHistoryComplete keeps its historical signature — main.go's
// handleHistorySync calls it on every ON_DEMAND chunk.
func markOnDemandHistoryComplete(completedAtMs int64, storedCount int) {
	onDemandHistory.complete(completedAtMs, storedCount)
}

type historySyncRequest struct {
	ChatJID   string `json:"chat_jid"`
	MessageID string `json:"message_id"`
	Timestamp string `json:"timestamp"`
	IsFromMe  bool   `json:"is_from_me"`
	Count     int    `json:"count"`
}

func historySyncMessageInfo(req historySyncRequest) (*types.MessageInfo, int, error) {
	if req.ChatJID == "" || req.MessageID == "" {
		return nil, 0, fmt.Errorf("chat_jid and message_id are required")
	}
	jid, err := types.ParseJID(req.ChatJID)
	if err != nil || jid.IsEmpty() {
		return nil, 0, fmt.Errorf("invalid chat_jid")
	}
	if jid.Server != types.DefaultUserServer && jid.Server != types.GroupServer {
		return nil, 0, fmt.Errorf("chat_jid must be a user or group JID")
	}
	timestamp, err := time.Parse(time.RFC3339, req.Timestamp)
	if err != nil {
		timestamp, err = time.Parse("2006-01-02 15:04:05Z07:00", req.Timestamp)
	}
	if err != nil {
		return nil, 0, fmt.Errorf("timestamp must be RFC3339")
	}
	if req.Count < 1 || req.Count > 1000 {
		return nil, 0, fmt.Errorf("count must be between 1 and 1000")
	}
	return &types.MessageInfo{
		MessageSource: types.MessageSource{
			Chat:     jid,
			IsFromMe: req.IsFromMe,
			IsGroup:  jid.Server == types.GroupServer,
		},
		ID:        types.MessageID(req.MessageID),
		Timestamp: timestamp,
	}, req.Count, nil
}

func registerHistorySyncHandler(
	mux *http.ServeMux,
	client *whatsmeow.Client,
	auth func(http.HandlerFunc) http.HandlerFunc,
) {
	mux.HandleFunc("/api/history-sync/status", auth(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		last, _ := onDemandHistory.snapshot(time.Now().UnixMilli())
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"completed_at_ms": last.CompletedAtMs,
			"stored_count":    last.StoredCount,
		})
	}))
	mux.HandleFunc("/api/history-sync", auth(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !client.IsConnected() || !client.IsLoggedIn() {
			http.Error(w, `{"error":"WhatsApp client is disconnected"}`, http.StatusServiceUnavailable)
			return
		}
		var req historySyncRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"invalid request format"}`, http.StatusBadRequest)
			return
		}
		info, count, err := historySyncMessageInfo(req)
		if err != nil {
			http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		requestedAtMs := time.Now().UnixMilli()
		response, err := client.SendPeerMessage(ctx, client.BuildHistorySyncRequest(info, count))
		if err != nil {
			http.Error(w, `{"error":"history request failed"}`, http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"accepted":        true,
			"request_id":      response.ID,
			"requested_at_ms": requestedAtMs,
			"chat_jid":        req.ChatJID,
			"count":           count,
		})
	}))
}
