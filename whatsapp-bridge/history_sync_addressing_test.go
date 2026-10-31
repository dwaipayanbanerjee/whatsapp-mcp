package main

import (
	"context"
	"testing"

	"go.mau.fi/whatsmeow/types"
)

func TestHistoryRequestChatSeparatesCanonicalIdentityFromRequestIdentity(t *testing.T) {
	pn := types.NewJID("15550000001", types.DefaultUserServer)
	lid := types.NewJID("900000000000001", types.HiddenUserServer)
	group := types.NewJID("15550000001-1", types.GroupServer)
	client := newTestClient(&mockLIDStore{lidByPN: map[types.JID]types.JID{pn: lid}})
	for _, tc := range []struct {
		name       string
		chat       types.JID
		addressing string
		want       types.JID
	}{
		{"mapped phone uses verified LID", pn, "", lid},
		{"explicit auto uses verified LID", pn, "auto", lid},
		{"bounded fallback retains phone", pn, "phone", pn},
		{"group remains group", group, "auto", group},
		{"unmapped phone remains phone", types.NewJID("15550000002", types.DefaultUserServer), "auto", types.NewJID("15550000002", types.DefaultUserServer)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := historyRequestChat(context.Background(), client, tc.chat, tc.addressing)
			if err != nil || got != tc.want {
				t.Fatalf("got %s, %v; want %s", got, err, tc.want)
			}
		})
	}
	if _, err := historyRequestChat(context.Background(), client, pn, "invented"); err == nil {
		t.Fatal("invalid addressing accepted")
	}
}

func TestHistoryRequestToSelfUsesPairedAccountLID(t *testing.T) {
	pn := types.NewJID("15550000001", types.DefaultUserServer)
	lid := types.NewJID("900000000000001", types.HiddenUserServer)
	client := newTestClientWithSelf(&mockLIDStore{}, pn)
	client.Store.LID = lid
	got, err := historyRequestChat(context.Background(), client, pn, "auto")
	if err != nil || got != lid {
		t.Fatalf("self history must use paired account LID: %s %v", got, err)
	}
	info, count, err := historySyncMessageInfo(historySyncRequest{ChatJID: pn.String(), MessageID: "native-note", Timestamp: "2026-10-28T06:25:17Z", IsFromMe: true, Count: 50})
	if err != nil {
		t.Fatal(err)
	}
	canonical := info.Chat
	info.Chat = got
	request := client.BuildHistorySyncRequest(info, count).GetProtocolMessage().GetPeerDataOperationRequestMessage().GetHistorySyncOnDemandRequest()
	if request.GetChatJID() != lid.String() || !request.GetOldestMsgFromMe() || request.GetOldestMsgID() != "native-note" || request.GetOnDemandMsgCount() != 50 {
		t.Fatalf("history wire request lost original anchor metadata: %v", request)
	}
	if canonical != pn {
		t.Fatal("canonical storage identity changed")
	}
	tracker := &onDemandTracker{}
	tracker.reserve("request", pn.String(), 1000, 60000)
	tracker.setRequestChat("request", lid.String())
	status := tracker.snapshot("request", 1001)
	if status.ChatJID != pn.String() || status.RequestChatJID != lid.String() {
		t.Fatalf("status mixed request and storage identity: %+v", status)
	}
}

func TestSelfHistoryResponseKeepsCanonicalArchiveChat(t *testing.T) {
	pn := types.NewJID("15550000001", types.DefaultUserServer)
	lid := types.NewJID("900000000000001", types.HiddenUserServer)
	client := newTestClientWithSelf(&mockLIDStore{}, pn)
	client.Store.LID = lid
	got := resolveLIDChat(client, lid, types.EmptyJID, types.EmptyJID, false)
	if got != pn {
		t.Fatalf("own LID history would split the archive chat: got %s want %s", got, pn)
	}
	ms := newTestMessageStore(t)
	history := testHistoryRows()
	id := lid.String()
	history.Data.Conversations[0].ID = &id
	result := handleHistorySync(client, ms, history, testLogger())
	if result.storageErrors != 0 || len(result.messages) != 1 || result.messages[0].ChatJID != pn.String() {
		t.Fatalf("history references must use canonical archive ID: %+v", result)
	}
}
