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

const (
	onDemandInflightTTLMs = 60_000
	onDemandStatusLimit   = 16
)

type historySyncStatus struct {
	ProtocolVersion int                 `json:"protocol_version"`
	State           string              `json:"state"`
	RequestID       string              `json:"request_id"`
	SessionID       string              `json:"session_id,omitempty"`
	ChatJID         string              `json:"chat_jid,omitempty"`
	RequestedAtMs   int64               `json:"requested_at_ms"`
	CompletedAtMs   int64               `json:"completed_at_ms"`
	StoredCount     int                 `json:"stored_count"`
	ChunksReceived  int                 `json:"chunks_received"`
	PendingChunks   int                 `json:"pending_chunks"`
	Progress        *uint32             `json:"progress,omitempty"`
	StorageErrors   int                 `json:"storage_errors"`
	MediaFailed     int                 `json:"media_failed"`
	DownloadErrors  int                 `json:"download_errors"`
	Completion      string              `json:"completion,omitempty"`
	Error           string              `json:"error,omitempty"`
	InFlight        bool                `json:"in_flight"`
	Messages        []historyMessageRef `json:"messages"`
}

type historyMessageRef struct {
	ChatJID   string `json:"chat_jid"`
	MessageID string `json:"message_id"`
}

type onDemandRecord struct {
	historySyncStatus
	expiresAtMs int64
	chunks      map[string]bool // false while processing; true after persistence/media finish
	messages    map[historyMessageRef]struct{}
}

// Notification session IDs must match the ID reserved before sending. Random
// request IDs prevent delayed responses (including after restart) from being
// attributed to a different request; only recent polling status needs retention.
type onDemandTracker struct {
	mu      sync.Mutex
	active  string
	records map[string]*onDemandRecord
	order   []string
}

func (t *onDemandTracker) expireLocked(nowMs int64) {
	r := t.records[t.active]
	if r != nil && r.PendingChunks == 0 && nowMs > r.expiresAtMs {
		if r.State == "pending" || r.State == "receiving" {
			r.State = "timed_out"
			r.Error = "history request timed out without explicit completion"
		}
		t.active = ""
	}
}

func (t *onDemandTracker) statusLocked(r *onDemandRecord) historySyncStatus {
	s := r.historySyncStatus
	s.InFlight = t.active == r.RequestID
	s.Messages = append([]historyMessageRef{}, r.Messages...)
	if s.Progress != nil {
		progress := *s.Progress
		s.Progress = &progress
	}
	return s
}

func (t *onDemandTracker) reserve(requestID, chatJID string, nowMs, ttlMs int64) *historySyncStatus {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.expireLocked(nowMs)
	if r := t.records[t.active]; r != nil {
		s := t.statusLocked(r)
		return &s
	}
	if r := t.records[requestID]; r != nil {
		s := t.statusLocked(r)
		return &s
	}
	if t.records == nil {
		t.records = make(map[string]*onDemandRecord)
	}
	t.records[requestID] = &onDemandRecord{
		historySyncStatus: historySyncStatus{
			ProtocolVersion: 2, State: "pending", RequestID: requestID,
			ChatJID: chatJID, RequestedAtMs: nowMs,
			Messages: []historyMessageRef{},
		},
		expiresAtMs: nowMs + ttlMs,
		chunks:      make(map[string]bool),
		messages:    make(map[historyMessageRef]struct{}),
	}
	t.active = requestID
	t.order = append(t.order, requestID)
	if len(t.order) > onDemandStatusLimit {
		delete(t.records, t.order[0])
		t.order = t.order[1:]
	}
	return nil
}

// Register before downloading so a terminal chunk cannot complete while another
// notified chunk is still being downloaded. A known duplicate returns true,false;
// an unknown or late new chunk returns false,false and may still be imported.
func (t *onDemandTracker) beginChunk(sessionID, chunkKey string, nowMs int64) (matched, accepted bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.expireLocked(nowMs)
	r := t.records[sessionID]
	if r == nil || sessionID == "" || chunkKey == "" {
		return false, false
	}
	if _, exists := r.chunks[chunkKey]; exists {
		return true, false
	}
	if t.active != sessionID || (r.State != "pending" && r.State != "receiving") {
		return false, false
	}
	r.SessionID = sessionID
	r.State = "receiving"
	r.chunks[chunkKey] = false
	r.ChunksReceived++
	r.PendingChunks++
	return true, true
}

func (t *onDemandTracker) settleLocked(r *onDemandRecord, nowMs int64) {
	if r.PendingChunks != 0 {
		return
	}
	if r.State == "failed" {
		t.active = ""
	} else if r.Progress != nil && *r.Progress == 100 {
		r.State = "completed"
		r.Completion = "explicit"
		r.CompletedAtMs = nowMs
		t.active = ""
	} else {
		t.expireLocked(nowMs)
	}
}

func (t *onDemandTracker) finishChunk(sessionID, chunkKey string, progress *uint32, storedCount, storageErrors, mediaFailed int, messages []historyMessageRef, nowMs int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	r := t.records[sessionID]
	if r == nil {
		return
	}
	finished, exists := r.chunks[chunkKey]
	if !exists || finished {
		return
	}
	r.chunks[chunkKey] = true
	r.PendingChunks--
	r.StoredCount += storedCount
	r.StorageErrors += storageErrors
	r.MediaFailed += mediaFailed
	for _, message := range messages {
		if _, exists := r.messages[message]; !exists {
			r.messages[message] = struct{}{}
			r.Messages = append(r.Messages, message)
		}
	}
	if progress != nil && (r.Progress == nil || *progress > *r.Progress) {
		value := *progress
		r.Progress = &value
	}
	if storageErrors != 0 || mediaFailed != 0 {
		r.State = "failed"
		r.Error = "history storage or media processing failed"
	}
	t.settleLocked(r, nowMs)
}

func (t *onDemandTracker) failChunk(sessionID, chunkKey string, nowMs int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	r := t.records[sessionID]
	if r == nil {
		return
	}
	finished, exists := r.chunks[chunkKey]
	if !exists || finished {
		return
	}
	r.chunks[chunkKey] = true
	r.PendingChunks--
	r.DownloadErrors++
	r.State = "failed"
	r.Error = "history chunk download or decoding failed"
	t.settleLocked(r, nowMs)
}

func (t *onDemandTracker) failRequest(requestID string, nowMs int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	r := t.records[requestID]
	if r == nil || (r.State != "pending" && r.State != "receiving") {
		return
	}
	r.State = "failed"
	r.Error = "history request send failed"
	t.settleLocked(r, nowMs)
}

func (t *onDemandTracker) snapshot(requestID string, nowMs int64) historySyncStatus {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.expireLocked(nowMs)
	if requestID == "" {
		requestID = t.active
		if requestID == "" && len(t.order) != 0 {
			requestID = t.order[len(t.order)-1]
		}
	}
	if r := t.records[requestID]; r != nil {
		return t.statusLocked(r)
	}
	return historySyncStatus{ProtocolVersion: 2, State: "unknown", RequestID: requestID}
}

var onDemandHistory = &onDemandTracker{}

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
			Chat: jid, IsFromMe: req.IsFromMe, IsGroup: jid.Server == types.GroupServer,
		},
		ID: types.MessageID(req.MessageID), Timestamp: timestamp,
	}, req.Count, nil
}

func registerHistorySyncHandler(mux *http.ServeMux, client *whatsmeow.Client, auth func(http.HandlerFunc) http.HandlerFunc) {
	mux.HandleFunc("/api/history-sync/status", auth(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		_ = json.NewEncoder(w).Encode(onDemandHistory.snapshot(r.URL.Query().Get("request_id"), time.Now().UnixMilli()))
	}))
	mux.HandleFunc("/api/history-sync", auth(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if client == nil || !client.IsConnected() || !client.IsLoggedIn() {
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
		requestedAtMs := time.Now().UnixMilli()
		requestID := client.GenerateMessageID()
		if blocking := onDemandHistory.reserve(string(requestID), req.ChatJID, requestedAtMs, onDemandInflightTTLMs); blocking != nil {
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"error": "history request already in flight", "request_id": blocking.RequestID, "chat_jid": blocking.ChatJID,
			})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		// SendPeerMessage generates its ID inside the send. Supplying it here
		// makes the reservation visible even if history arrives before the ACK.
		_, err = client.SendMessage(ctx, client.Store.GetJID().ToNonAD(), client.BuildHistorySyncRequest(info, count), whatsmeow.SendRequestExtra{Peer: true, ID: requestID})
		if err != nil {
			onDemandHistory.failRequest(string(requestID), time.Now().UnixMilli())
			if onDemandHistory.snapshot(string(requestID), time.Now().UnixMilli()).State != "completed" {
				http.Error(w, `{"error":"history request failed"}`, http.StatusBadGateway)
				return
			}
		}
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"accepted": true, "protocol_version": 2, "request_id": requestID,
			"requested_at_ms": requestedAtMs, "chat_jid": req.ChatJID, "count": count,
		})
	}))
}
