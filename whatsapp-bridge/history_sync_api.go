package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

var onDemandHistoryCompletedAtMs atomic.Int64
var onDemandHistoryStoredCount atomic.Int64

func markOnDemandHistoryComplete(completedAtMs int64, storedCount int) {
	onDemandHistoryStoredCount.Store(int64(storedCount))
	onDemandHistoryCompletedAtMs.Store(completedAtMs)
}

func onDemandHistoryStatus() (int64, int64) {
	return onDemandHistoryCompletedAtMs.Load(), onDemandHistoryStoredCount.Load()
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
		completedAtMs, storedCount := onDemandHistoryStatus()
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"completed_at_ms": completedAtMs,
			"stored_count":    storedCount,
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
