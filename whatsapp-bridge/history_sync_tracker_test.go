package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func historyProgress(value uint32) *uint32 { return &value }

func reserveHistory(t *testing.T, tr *onDemandTracker, id string, nowMs int64) {
	t.Helper()
	if blocked := tr.reserve(id, id+"@s.whatsapp.net", nowMs, onDemandInflightTTLMs); blocked != nil {
		t.Fatalf("reserve %s blocked: %+v", id, blocked)
	}
}

func beginHistoryChunk(t *testing.T, tr *onDemandTracker, id, key string, nowMs int64) {
	t.Helper()
	if matched, accepted := tr.beginChunk(id, key, nowMs); !matched || !accepted {
		t.Fatalf("expected matching new chunk %s/%s, got %v,%v", id, key, matched, accepted)
	}
}

func TestTrackerSerializesRequestsWithPreallocatedID(t *testing.T) {
	tr := &onDemandTracker{}
	reserveHistory(t, tr, "REQ-A", 1000)
	blocked := tr.reserve("REQ-B", "b@s.whatsapp.net", 2000, onDemandInflightTTLMs)
	if blocked == nil || blocked.RequestID != "REQ-A" || !blocked.InFlight {
		t.Fatalf("blocking record must expose the ID before send returns: %+v", blocked)
	}
	beginHistoryChunk(t, tr, "REQ-A", "chunk", 2001)
	tr.finishChunk("REQ-A", "chunk", historyProgress(100), 3, 0, 0, nil, 2002)
	// A late ACK failure cannot erase an already persisted explicit response.
	tr.failRequest("REQ-A", 2003)
	status := tr.snapshot("REQ-A", 2004)
	if status.State != "completed" || status.SessionID != "REQ-A" || status.Completion != "explicit" || status.StoredCount != 3 || status.CompletedAtMs != 2002 {
		t.Fatalf("early response was lost: %+v", status)
	}
	reserveHistory(t, tr, "REQ-B", 2005)
}

func TestTrackerExpiryCannotAttributeLateAToNewB(t *testing.T) {
	tr := &onDemandTracker{}
	reserveHistory(t, tr, "REQ-A", 1000)
	reserveHistory(t, tr, "REQ-B", 61001)
	if status := tr.snapshot("REQ-A", 61002); status.State != "timed_out" || status.CompletedAtMs != 0 || status.InFlight {
		t.Fatalf("expired request must retain failure: %+v", status)
	}
	for _, id := range []string{"REQ-A", "", "unrelated"} {
		if matched, accepted := tr.beginChunk(id, "late", 61003); matched || accepted {
			t.Fatalf("late/unrelated new chunk should import unattributed: %q %v,%v", id, matched, accepted)
		}
		tr.finishChunk(id, "late", historyProgress(100), 100, 0, 0, nil, 61004)
	}
	if status := tr.snapshot("REQ-B", 61005); status.State != "pending" || status.StoredCount != 0 || status.SessionID != "" {
		t.Fatalf("late A consumed B: %+v", status)
	}
}

func TestTrackerRestartCannotAttributeOldResponse(t *testing.T) {
	tr := &onDemandTracker{}
	reserveHistory(t, tr, "NEW-ID", 1000)
	if matched, accepted := tr.beginChunk("OLD-ID", "old-chunk", 1001); matched || accepted {
		t.Fatal("response to a previous process must remain unattributed")
	}
	tr.finishChunk("OLD-ID", "old-chunk", historyProgress(100), 9, 0, 0, nil, 1002)
	if status := tr.snapshot("NEW-ID", 1003); status.State != "pending" || status.StoredCount != 0 {
		t.Fatalf("old response affected current request: %+v", status)
	}
}

func TestTrackerRequiresExplicitTerminalProgress(t *testing.T) {
	for _, progress := range []*uint32{nil, historyProgress(0), historyProgress(50), historyProgress(99), historyProgress(101)} {
		t.Run(fmt.Sprint(progress), func(t *testing.T) {
			tr := &onDemandTracker{}
			reserveHistory(t, tr, "REQ", 1000)
			beginHistoryChunk(t, tr, "REQ", "one", 1001)
			tr.finishChunk("REQ", "one", progress, 10, 0, 0, nil, 1002)
			if status := tr.snapshot("REQ", 1003); status.State != "receiving" || !status.InFlight || status.Completion != "" || status.CompletedAtMs != 0 {
				t.Fatalf("nonterminal chunk reported completion: %+v", status)
			}
			if status := tr.snapshot("REQ", 61001); status.State != "timed_out" || status.Completion != "" {
				t.Fatalf("quiet timeout must never report completion: %+v", status)
			}
		})
	}
}

func TestTrackerWaitsForAllNotifiedChunksAndMedia(t *testing.T) {
	tr := &onDemandTracker{}
	reserveHistory(t, tr, "REQ", 1000)
	beginHistoryChunk(t, tr, "REQ", "earlier", 1001)
	beginHistoryChunk(t, tr, "REQ", "terminal", 1002)
	tr.finishChunk("REQ", "terminal", historyProgress(100), 5, 0, 0, nil, 1003)
	status := tr.snapshot("REQ", 70000)
	if status.State != "receiving" || status.PendingChunks != 1 || status.ChunksReceived != 2 || !status.InFlight || status.CompletedAtMs != 0 {
		t.Fatalf("terminal chunk must wait for earlier storage/media even beyond TTL: %+v", status)
	}
	if blocked := tr.reserve("NEXT", "b@s.whatsapp.net", 70000, onDemandInflightTTLMs); blocked == nil {
		t.Fatal("slow processing must keep its slot beyond TTL")
	}
	tr.finishChunk("REQ", "earlier", historyProgress(50), 8, 0, 0, nil, 70001)
	status = tr.snapshot("REQ", 70002)
	if status.State != "completed" || status.StoredCount != 13 || status.PendingChunks != 0 || status.InFlight || status.Progress == nil || *status.Progress != 100 {
		t.Fatalf("all persisted chunks should complete: %+v", status)
	}
	// Snapshot callers cannot mutate the tracker's progress through its pointer.
	*status.Progress = 0
	if *tr.snapshot("REQ", 70002).Progress != 100 {
		t.Fatal("snapshot leaked mutable tracker progress")
	}
}

func TestTrackerDuplicateChunksDoNotDoubleCount(t *testing.T) {
	tr := &onDemandTracker{}
	reserveHistory(t, tr, "REQ", 1000)
	beginHistoryChunk(t, tr, "REQ", "one", 1001)
	if matched, accepted := tr.beginChunk("REQ", "one", 1002); !matched || accepted {
		t.Fatal("in-progress duplicate must be recognized and skipped")
	}
	tr.finishChunk("REQ", "one", historyProgress(100), 7, 0, 0, nil, 1003)
	tr.finishChunk("REQ", "one", historyProgress(100), 7, 1, 1, nil, 1004)
	tr.failChunk("REQ", "one", 1005)
	if matched, accepted := tr.beginChunk("REQ", "one", 1006); !matched || accepted {
		t.Fatal("completed duplicate must be recognized and skipped")
	}
	status := tr.snapshot("REQ", 1007)
	if status.State != "completed" || status.StoredCount != 7 || status.ChunksReceived != 1 || status.PendingChunks != 0 || status.DownloadErrors+status.StorageErrors+status.MediaFailed != 0 {
		t.Fatalf("duplicate changed completion: %+v", status)
	}
}

func TestTrackerRetainsExactSuccessfulMessageReferences(t *testing.T) {
	tr := &onDemandTracker{}
	reserveHistory(t, tr, "REQ", 1000)
	beginHistoryChunk(t, tr, "REQ", "one", 1001)
	beginHistoryChunk(t, tr, "REQ", "two", 1002)
	first := historyMessageRef{ChatJID: "one@s.whatsapp.net", MessageID: "message-1"}
	second := historyMessageRef{ChatJID: "two@g.us", MessageID: "message-1"}
	tr.finishChunk("REQ", "one", historyProgress(50), 1, 0, 0, []historyMessageRef{first}, 1003)
	tr.finishChunk("REQ", "two", historyProgress(100), 2, 0, 0, []historyMessageRef{first, second}, 1004)
	status := tr.snapshot("REQ", 1005)
	if status.State != "completed" || len(status.Messages) != 2 || status.Messages[0] != first || status.Messages[1] != second {
		t.Fatalf("reference identity must include chat and deduplicate repeated upserts: %+v", status)
	}
	status.Messages[0].MessageID = "changed"
	if tr.snapshot("REQ", 1006).Messages[0] != first {
		t.Fatal("snapshot leaked mutable message references")
	}
}

func TestTrackerProcessingFailuresAreNotSuccess(t *testing.T) {
	for _, failure := range []string{"storage", "media", "download", "send"} {
		t.Run(failure, func(t *testing.T) {
			tr := &onDemandTracker{}
			reserveHistory(t, tr, "REQ", 1000)
			beginHistoryChunk(t, tr, "REQ", "failing", 1001)
			beginHistoryChunk(t, tr, "REQ", "terminal", 1002)
			switch failure {
			case "storage":
				tr.finishChunk("REQ", "failing", nil, 1, 1, 0, nil, 1003)
			case "media":
				tr.finishChunk("REQ", "failing", nil, 1, 0, 1, nil, 1003)
			case "download":
				tr.failChunk("REQ", "failing", 1003)
			case "send":
				tr.failRequest("REQ", 1003)
				tr.finishChunk("REQ", "failing", nil, 1, 0, 0, nil, 1004)
			}
			if status := tr.snapshot("REQ", 1005); status.State != "failed" || !status.InFlight {
				t.Fatalf("failed processing must retain slot until pending work ends: %+v", status)
			}
			tr.finishChunk("REQ", "terminal", historyProgress(100), 2, 0, 0, nil, 1006)
			status := tr.snapshot("REQ", 1007)
			if status.State != "failed" || status.CompletedAtMs != 0 || status.Completion != "" || status.PendingChunks != 0 || status.InFlight {
				t.Fatalf("terminal chunk concealed failure: %+v", status)
			}
			reserveHistory(t, tr, "NEXT", 1008)
			if matched, accepted := tr.beginChunk("REQ", "late", 1009); matched || accepted {
				t.Fatal("new late response to failed request should import unattributed")
			}
		})
	}
}

func TestTrackerSendFailureReopensSlot(t *testing.T) {
	tr := &onDemandTracker{}
	reserveHistory(t, tr, "REQ-A", 1000)
	tr.failRequest("REQ-A", 1001)
	reserveHistory(t, tr, "REQ-B", 1002)
	if status := tr.snapshot("REQ-A", 1003); status.State != "failed" || status.CompletedAtMs != 0 {
		t.Fatalf("send failure must remain observable: %+v", status)
	}
}

func TestTrackerKeepsBoundedRecentStatuses(t *testing.T) {
	tr := &onDemandTracker{}
	for index := 0; index < onDemandStatusLimit+2; index++ {
		id := fmt.Sprintf("REQ-%d", index)
		reserveHistory(t, tr, id, int64(index+1000))
		tr.failRequest(id, int64(index+1001))
	}
	if status := tr.snapshot("REQ-0", 2000); status.State != "unknown" || status.CompletedAtMs != 0 {
		t.Fatalf("forgotten ID must fail closed: %+v", status)
	}
	if len(tr.records) != onDemandStatusLimit || len(tr.order) != onDemandStatusLimit {
		t.Fatal("tracker status retention grew beyond its limit")
	}
	if status := tr.snapshot("", 2000); status.RequestID != fmt.Sprintf("REQ-%d", onDemandStatusLimit+1) {
		t.Fatalf("default status should use latest when idle: %+v", status)
	}
}

func TestTrackerConcurrentDuplicateRegistration(t *testing.T) {
	tr := &onDemandTracker{}
	reserveHistory(t, tr, "REQ", 1000)
	var acceptedCount atomic.Int32
	var group sync.WaitGroup
	for index := 0; index < 20; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			if _, accepted := tr.beginChunk("REQ", "same", 1001); accepted {
				acceptedCount.Add(1)
			}
			_ = tr.snapshot("REQ", 1002)
		}()
	}
	group.Wait()
	if acceptedCount.Load() != 1 || tr.snapshot("REQ", 1003).PendingChunks != 1 {
		t.Fatal("concurrent duplicate notifications must register exactly once")
	}
}

func TestHistoryStatusAPIQueriesExactRequest(t *testing.T) {
	previous := onDemandHistory
	onDemandHistory = &onDemandTracker{}
	t.Cleanup(func() { onDemandHistory = previous })
	nowMs := time.Now().UnixMilli()
	reserveHistory(t, onDemandHistory, "REQ-A", nowMs)
	beginHistoryChunk(t, onDemandHistory, "REQ-A", "one", nowMs)
	onDemandHistory.finishChunk("REQ-A", "one", historyProgress(100), 4, 0, 0, nil, nowMs)
	reserveHistory(t, onDemandHistory, "REQ-B", nowMs)
	mux := http.NewServeMux()
	registerHistorySyncHandler(mux, nil, func(handler http.HandlerFunc) http.HandlerFunc { return handler })
	for _, check := range []struct{ query, wantID, wantState string }{
		{"?request_id=REQ-A", "REQ-A", "completed"},
		{"?request_id=REQ-B", "REQ-B", "pending"},
		{"?request_id=unknown", "unknown", "unknown"},
		{"", "REQ-B", "pending"},
	} {
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/history-sync/status"+check.query, nil))
		var status historySyncStatus
		if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil {
			t.Fatal(err)
		}
		if response.Code != http.StatusOK || status.ProtocolVersion != 2 || status.RequestID != check.wantID || status.State != check.wantState {
			t.Fatalf("status query %q: %+v (HTTP %d)", check.query, status, response.Code)
		}
	}
}
