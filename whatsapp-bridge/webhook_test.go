package main

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

// TestMain keeps host-level opt-out configuration from silently changing tests
// that exercise webhook delivery. Individual tests set WEBHOOK_ENABLED when
// they need to cover an enabled or disabled value.
func TestMain(m *testing.M) {
	_ = os.Unsetenv("WEBHOOK_ENABLED")
	os.Exit(m.Run())
}

func unsetWebhookEnabled(t *testing.T) {
	t.Helper()
	value, wasSet := os.LookupEnv("WEBHOOK_ENABLED")
	if err := os.Unsetenv("WEBHOOK_ENABLED"); err != nil {
		t.Fatalf("unset WEBHOOK_ENABLED: %v", err)
	}
	t.Cleanup(func() {
		if wasSet {
			_ = os.Setenv("WEBHOOK_ENABLED", value)
		} else {
			_ = os.Unsetenv("WEBHOOK_ENABLED")
		}
	})
}

// setWebhookAuthToken sets the package-level outbound webhook token for the
// duration of a test and restores the previous value on cleanup.
func setWebhookAuthToken(t *testing.T, token string) {
	t.Helper()
	prev := webhookAuthToken
	webhookAuthToken = token
	t.Cleanup(func() { webhookAuthToken = prev })
}

// TestSendWebhookDisabledByEnv verifies that WEBHOOK_ENABLED=false suppresses
// outbound webhooks even when WEBHOOK_URL is set.
func TestSendWebhookDisabledByEnv(t *testing.T) {
	var received bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received = true
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	t.Setenv("WEBHOOK_ENABLED", "false")
	t.Setenv("WEBHOOK_URL", srv.URL)

	SendWebhook("123@s.whatsapp.net", "hi", "123@s.whatsapp.net", false, "", "", "", nil, nil)

	if received {
		t.Fatal("webhook was delivered despite WEBHOOK_ENABLED=false")
	}
}

// TestSendWebhookEnabledByDefault guards the default: omitting WEBHOOK_ENABLED
// must leave delivery behavior exactly as it was before the flag existed.
func TestSendWebhookEnabledByDefault(t *testing.T) {
	unsetWebhookEnabled(t)
	var received bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received = true
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	t.Setenv("WEBHOOK_URL", srv.URL)

	SendWebhook("123@s.whatsapp.net", "hi", "123@s.whatsapp.net", false, "", "", "", nil, nil)

	if !received {
		t.Fatal("webhook was not delivered with WEBHOOK_ENABLED unset")
	}
}

// TestSendWebhookWithMessageIDSerializesID verifies text-only inbound messages
// preserve their WhatsApp message ID so downstream webhook idempotency can
// discard duplicate WhatsApp event deliveries.
func TestSendWebhookWithMessageIDSerializesID(t *testing.T) {
	var payload WebhookPayload
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { _ = r.Body.Close() }()
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode webhook payload: %v", err)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	t.Setenv("WEBHOOK_URL", srv.URL)
	SendWebhookWithMessageID("123@s.whatsapp.net", "hi", "123@s.whatsapp.net", false, "", "", "", nil, nil, "3EB0F00D")

	if payload.MessageID != "3EB0F00D" {
		t.Fatalf("messageId = %q, want %q", payload.MessageID, "3EB0F00D")
	}
}

func TestSendWebhookWithMediaDisabledSkipsMediaIO(t *testing.T) {
	var received bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received = true
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	t.Setenv("WEBHOOK_ENABLED", "false")
	t.Setenv("WEBHOOK_URL", srv.URL)
	missingPath := filepath.Join(t.TempDir(), "missing.jpg")

	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatalf("create stdout pipe: %v", err)
	}
	previousStdout := os.Stdout
	os.Stdout = writeEnd
	SendWebhookWithMedia(
		"123@s.whatsapp.net", "", "123@s.whatsapp.net", false,
		"", "", "", nil, nil,
		"message-id", "image", "image/jpeg", "missing.jpg", missingPath,
	)
	_ = writeEnd.Close()
	os.Stdout = previousStdout
	output, err := io.ReadAll(readEnd)
	if err != nil {
		t.Fatalf("read captured stdout: %v", err)
	}
	_ = readEnd.Close()

	if received {
		t.Fatal("webhook was delivered despite WEBHOOK_ENABLED=false")
	}
	if strings.Contains(string(output), "Could not stat media file") {
		t.Fatalf("disabled webhook still touched media path: %s", output)
	}
}

// TestSendWebhookAttachesBridgeTokenHeader verifies that outbound webhook POSTs
// carry the shared bridge token as an "X-Bridge-Token" header so the hub's
// fail-closed inbound-auth middleware (autohub PR #898) accepts them. The token
// travels on a dedicated header, not Authorization, so it never collides with a
// receiver's own Authorization-based auth (see
// TestSendWebhookPreservesURLBasicAuth).
func TestSendWebhookAttachesBridgeTokenHeader(t *testing.T) {
	const token = "test-bridge-token-1234567890abcdef"

	var gotToken, gotContentType string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotToken = r.Header.Get("X-Bridge-Token")
		gotContentType = r.Header.Get("Content-Type")
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	t.Setenv("WEBHOOK_URL", srv.URL)
	setWebhookAuthToken(t, token)

	SendWebhook("123@s.whatsapp.net", "hello", "123@s.whatsapp.net", false, "", "", "", nil, nil)

	if gotToken != token {
		t.Fatalf("X-Bridge-Token header = %q, want %q", gotToken, token)
	}
	if gotContentType != "application/json" {
		t.Fatalf("Content-Type header = %q, want application/json", gotContentType)
	}
}

// TestSendWebhookOmitsBridgeTokenHeaderWhenNoToken verifies that when no bridge
// token is configured the webhook still fires WITHOUT an X-Bridge-Token header,
// so deployments that predate the token rollout keep working.
func TestSendWebhookOmitsBridgeTokenHeaderWhenNoToken(t *testing.T) {
	var gotToken string
	var received bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received = true
		gotToken = r.Header.Get("X-Bridge-Token")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	t.Setenv("WEBHOOK_URL", srv.URL)
	setWebhookAuthToken(t, "")

	SendWebhook("123@s.whatsapp.net", "hi", "123@s.whatsapp.net", false, "", "", "", nil, nil)

	if !received {
		t.Fatal("webhook was not delivered")
	}
	if gotToken != "" {
		t.Fatalf("expected no X-Bridge-Token header, got %q", gotToken)
	}
}

// TestSendWebhookPreservesURLBasicAuth is a regression test for a Codex review
// finding on PR #153: net/http automatically derives an "Authorization: Basic"
// header from credentials embedded in the webhook URL (http://user:pass@host/...)
// whenever the outgoing request's Authorization header is otherwise unset. An
// earlier version of this fix sent the bridge token via Authorization, which
// silently clobbered that behavior for any receiver relying on URL userinfo.
// Sending the token as X-Bridge-Token instead must leave Authorization
// untouched so Go's built-in URL-credential handling keeps working.
func TestSendWebhookPreservesURLBasicAuth(t *testing.T) {
	const user, pass = "hookuser", "hookpass"
	const token = "test-bridge-token-1234567890abcdef"

	var gotAuth, gotToken string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotToken = r.Header.Get("X-Bridge-Token")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("failed to parse test server URL: %v", err)
	}
	u.User = url.UserPassword(user, pass)

	t.Setenv("WEBHOOK_URL", u.String())
	setWebhookAuthToken(t, token)

	SendWebhook("123@s.whatsapp.net", "hello", "123@s.whatsapp.net", false, "", "", "", nil, nil)

	wantAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))
	if gotAuth != wantAuth {
		t.Fatalf("Authorization header = %q, want %q (URL basic auth must survive)", gotAuth, wantAuth)
	}
	if gotToken != token {
		t.Fatalf("X-Bridge-Token header = %q, want %q", gotToken, token)
	}
}

// The unset case listens on the legacy default port (8769): code that falls
// back to http://localhost:8769/whatsapp/webhook fails this test, which pins
// the "unset means no POST" (opt-in) contract.
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

	unsetWebhookEnabled(t)
	t.Setenv("WEBHOOK_URL", "")
	setWebhookAuthToken(t, "test-bridge-token-1234567890abcdef")
	SendWebhook("123@s.whatsapp.net", "hello", "123@s.whatsapp.net", false, "", "", "", nil, nil)
	if hits.Load() != 0 {
		t.Fatalf("expected no webhook POST when WEBHOOK_URL is unset, got %d", hits.Load())
	}
}

// TestSendWebhookDoesNotFollowRedirects is a regression test for a Codex
// review finding on PR #153: Go's default http.Client follows redirects and
// forwards custom headers to the redirect target regardless of host, unlike
// Authorization/Cookie which it strips cross-origin. A misconfigured or
// malicious WEBHOOK_URL that responds with a 3xx could otherwise cause the
// bridge to leak X-Bridge-Token to an arbitrary third-party host. The webhook
// client must not follow redirects at all, so a second host is never
// contacted.
func TestSendWebhookDoesNotFollowRedirects(t *testing.T) {
	const token = "test-bridge-token-1234567890abcdef"

	var targetHit bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetHit = true
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	var redirectHit bool
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirectHit = true
		http.Redirect(w, r, target.URL+"/whatsapp/webhook", http.StatusFound)
	}))
	defer redirector.Close()

	t.Setenv("WEBHOOK_URL", redirector.URL)
	setWebhookAuthToken(t, token)

	SendWebhook("123@s.whatsapp.net", "hi", "123@s.whatsapp.net", false, "", "", "", nil, nil)
	if !redirectHit {
		t.Fatal("expected the configured webhook URL to be hit")
	}
	if targetHit {
		t.Fatal("bridge must not follow redirects to a different host (would leak X-Bridge-Token)")
	}
}

func TestSendWebhookSerializesNativeMentionAndQuotedOrigin(t *testing.T) {
	var payload WebhookPayload
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { _ = r.Body.Close() }()
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode webhook payload: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	t.Setenv("WEBHOOK_URL", srv.URL)
	quotedIsFromMe := true
	quotedOrigin := &quotedIsFromMe
	SendWebhook(
		"123@s.whatsapp.net", "hello", "123@s.whatsapp.net", false,
		"quoted-id", "456@s.whatsapp.net", "[🤖] prior response",
		quotedOrigin, []string{"491742555497@s.whatsapp.net"},
	)

	if payload.QuotedIsFromMe == nil || !*payload.QuotedIsFromMe {
		t.Fatalf("quotedIsFromMe = %v, want true", payload.QuotedIsFromMe)
	}
	if len(payload.MentionedJIDs) != 1 || payload.MentionedJIDs[0] != "491742555497@s.whatsapp.net" {
		t.Fatalf("mentionedJids = %#v", payload.MentionedJIDs)
	}
}
