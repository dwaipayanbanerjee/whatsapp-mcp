package main

import "testing"

func TestTrackerSerializesRequests(t *testing.T) {
	tr := &onDemandTracker{}
	if blocked := tr.reserve("a@s.whatsapp.net", 1000, 60000); blocked != nil {
		t.Fatal("first reserve must succeed")
	}
	tr.commit("REQ-A")
	blocked := tr.reserve("b@s.whatsapp.net", 2000, 60000)
	if blocked == nil {
		t.Fatal("second reserve while in flight must be blocked")
	}
	if blocked.RequestID != "REQ-A" || blocked.ChatJID != "a@s.whatsapp.net" {
		t.Fatalf("blocking record should describe the in-flight request, got %+v", blocked)
	}
}

func TestTrackerExpiryReopensSlot(t *testing.T) {
	tr := &onDemandTracker{}
	tr.reserve("a@s.whatsapp.net", 1000, 60000)
	tr.commit("REQ-A")
	if blocked := tr.reserve("b@s.whatsapp.net", 1000+60000+1, 60000); blocked != nil {
		t.Fatalf("expired in-flight must not block, got %+v", blocked)
	}
}

func TestTrackerReleaseReopensSlot(t *testing.T) {
	tr := &onDemandTracker{}
	tr.reserve("a@s.whatsapp.net", 1000, 60000)
	tr.release()
	if blocked := tr.reserve("b@s.whatsapp.net", 1001, 60000); blocked != nil {
		t.Fatal("release must reopen the slot")
	}
}

func TestTrackerCompleteConsumesInflight(t *testing.T) {
	tr := &onDemandTracker{}
	tr.reserve("a@s.whatsapp.net", 1000, 60000)
	tr.commit("REQ-A")
	tr.complete(5000, 49)
	last, inflight := tr.snapshot(5001)
	if inflight != nil {
		t.Fatal("completion must clear the in-flight slot")
	}
	want := onDemandCompletion{
		RequestID: "REQ-A", ChatJID: "a@s.whatsapp.net",
		RequestedAtMs: 1000, CompletedAtMs: 5000, StoredCount: 49,
	}
	if last != want {
		t.Fatalf("completion record mismatch: got %+v want %+v", last, want)
	}
}

func TestTrackerCompletionWithoutReservation(t *testing.T) {
	tr := &onDemandTracker{}
	tr.complete(5000, 12)
	last, _ := tr.snapshot(5001)
	if last.RequestID != "" || last.ChatJID != "" {
		t.Fatalf("unsolicited completion must have empty attribution, got %+v", last)
	}
	if last.CompletedAtMs != 5000 || last.StoredCount != 12 {
		t.Fatalf("unsolicited completion must still record time and count, got %+v", last)
	}
}

func TestTrackerSnapshotHidesExpiredInflight(t *testing.T) {
	tr := &onDemandTracker{}
	tr.reserve("a@s.whatsapp.net", 1000, 60000)
	tr.commit("REQ-A")
	if _, inflight := tr.snapshot(2000); inflight == nil {
		t.Fatal("unexpired in-flight must be visible")
	}
	if _, inflight := tr.snapshot(1000 + 60000 + 1); inflight != nil {
		t.Fatal("expired in-flight must be hidden")
	}
}
