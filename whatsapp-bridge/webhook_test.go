package main

import (
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// The unset case listens on the LEGACY default port (8769): the pre-change
// code falls back to http://localhost:8769/whatsapp/webhook, so this test
// fails against that behavior and pins the "unset means no POST" contract.
func TestSendWebhookSkipsWhenURLUnset(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:8769")
	if err != nil {
		t.Skipf("legacy webhook port unavailable: %v", err)
	}
	var hits atomic.Int64
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
	})}
	go func() { _ = server.Serve(listener) }()
	defer func() { _ = server.Close() }()

	t.Setenv("WEBHOOK_URL", "")
	SendWebhook("123@s.whatsapp.net", "hello", "123@s.whatsapp.net", false, "", "", "")
	if hits.Load() != 0 {
		t.Fatalf("expected no webhook POST when WEBHOOK_URL is unset, got %d", hits.Load())
	}
}

func TestSendWebhookPostsWhenURLSet(t *testing.T) {
	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/whatsapp/webhook" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		hits.Add(1)
	}))
	defer server.Close()

	t.Setenv("WEBHOOK_URL", server.URL+"/whatsapp/webhook")
	SendWebhook("123@s.whatsapp.net", "hello", "123@s.whatsapp.net", false, "", "", "")
	if hits.Load() != 1 {
		t.Fatalf("expected exactly one webhook POST, got %d", hits.Load())
	}
}
