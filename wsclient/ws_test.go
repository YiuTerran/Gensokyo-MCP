package wsclient

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hoshinonyaruko/gensokyo-mcp/bridge"
	"github.com/hoshinonyaruko/gensokyo-mcp/callapi"
)

func TestBridgeOutputTargetInfersOnlyUnambiguousRecipients(t *testing.T) {
	group, groupID, ok := bridgeOutputTarget("send_msg", json.RawMessage(`42`), nil, "")
	if !ok || group != "group" || groupID != 42 {
		t.Fatalf("group inference: %q %d %v", group, groupID, ok)
	}
	private, userID, ok := bridgeOutputTarget("send_msg", nil, json.RawMessage(`43`), "")
	if !ok || private != "private" || userID != 43 {
		t.Fatalf("private inference: %q %d %v", private, userID, ok)
	}
	if _, _, ok := bridgeOutputTarget("send_msg", json.RawMessage(`42`), json.RawMessage(`43`), ""); ok {
		t.Fatal("ambiguous send_msg recipient accepted")
	}
	if _, _, ok := bridgeOutputTarget("send_msg", nil, nil, ""); ok {
		t.Fatal("recipient-less send_msg accepted")
	}
	if _, _, ok := bridgeOutputTarget("send_group_msg", json.RawMessage(`"42"`), nil, ""); ok {
		t.Fatal("numeric string wire ID accepted")
	}
}

func TestBridgeClientKeepsRetryingUntilBackendStartsAndCloseStopsIt(t *testing.T) {
	manager, err := bridge.Open(t.TempDir() + "/bridge.db")
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	SetBridgeManager(manager)
	defer SetBridgeManager(nil)

	var accepting atomic.Bool
	var handshakes atomic.Int32
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !accepting.Load() {
			http.Error(w, "starting", http.StatusServiceUnavailable)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		handshakes.Add(1)
		defer conn.Close()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	url := "ws" + server.URL[len("http"):]
	start := time.Now()
	client, err := NewWebSocketClient(url, 123, 1, "sealdice")
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("bridge client blocked HTTP startup on the initial WebSocket connection")
	}
	// The old retry=1 implementation stopped after its second failure at 5s.
	time.Sleep(6 * time.Second)
	accepting.Store(true)
	deadline := time.Now().Add(5 * time.Second)
	for handshakes.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(25 * time.Millisecond)
	}
	if handshakes.Load() == 0 {
		t.Fatal("bridge client did not connect after backend became available")
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	closedCount := handshakes.Load()
	time.Sleep(1200 * time.Millisecond)
	if got := handshakes.Load(); got != closedCount {
		t.Fatalf("client retried after Close: handshakes %d -> %d", closedCount, got)
	}
}

func TestBridgeReplyCorrelationSupportsArrayAndEscapedCQText(t *testing.T) {
	array := json.RawMessage(`[{"type":"text","data":{"text":"before"}},{"type":"reply","data":{"id":"73"}},{"type":"text","data":{"text":" after"}}]`)
	text, id, ok := extractCorrelatedText(array)
	if !ok || text != "before after" || id != 73 {
		t.Fatalf("array correlation: %q %d %v", text, id, ok)
	}
	if _, id, ok := extractCorrelatedText(json.RawMessage(`[{"type":"reply","data":{"id":73}}]`)); !ok || id != 73 {
		t.Fatalf("numeric reply ID compatibility: %d %v", id, ok)
	}
	text, id, ok = extractCorrelatedText(json.RawMessage(`"hello[CQ:reply,id=74] &amp; &#91;literal&#93;"`))
	if !ok || text != "hello & [literal]" || id != 74 {
		t.Fatalf("CQ correlation: %q %d %v", text, id, ok)
	}
	text, id, ok = extractCorrelatedText(json.RawMessage(`"&#91;CQ:reply&#93;[CQ:reply,id=75]"`))
	if !ok || text != "[CQ:reply]" || id != 75 {
		t.Fatalf("escaped pseudo reply: %q %d %v", text, id, ok)
	}
	for _, raw := range []json.RawMessage{
		json.RawMessage(`"hello [CQ:reply]"`),
		json.RawMessage(`"hello[CQ:image,file=x][CQ:reply,id=1]"`),
		json.RawMessage(`"hello[CQ:reply,id=2147483648]"`),
	} {
		if _, _, ok := extractCorrelatedText(raw); ok {
			t.Fatalf("invalid CQ message accepted: %s", raw)
		}
	}
	for _, raw := range []json.RawMessage{
		json.RawMessage(`[{"type":"reply","data":{"id":"+1"}}]`),
		json.RawMessage(`[{"type":"reply","data":{"id":"01"}}]`),
		json.RawMessage(`[{"type":"reply","data":{"id":"1.0"}}]`),
		json.RawMessage(`[{"type":"reply","data":{"id":"2147483648"}}]`),
	} {
		if _, _, ok := extractCorrelatedText(raw); ok {
			t.Fatalf("malformed string reply ID accepted: %s", raw)
		}
	}
	if _, _, ok := extractCorrelatedTextWithOptions(json.RawMessage(`"[CQ:reply,id=76]"`), true); ok {
		t.Fatal("auto_escape string was parsed as a reply")
	}
}

func TestBridgeActionDecodePreservesArbitraryEchoJSON(t *testing.T) {
	var wire actionEnvelope
	if err := json.Unmarshal([]byte(`{"action":"send_msg","params":{"group_id":1,"message":[{"type":"reply","data":{"id":9}},{"type":"text","data":{"text":"ok"}}]},"echo":{"kind":[true,null,3]}}`), &wire); err != nil {
		t.Fatal(err)
	}
	var value map[string]interface{}
	if err := json.Unmarshal(echoValue(wire.Echo).(json.RawMessage), &value); err != nil {
		t.Fatal(err)
	}
	if value["kind"] == nil {
		t.Fatalf("echo not preserved: %s", wire.Echo)
	}
}

func TestLegacySendActionsReturnStandardACKAndPersistInt32MessageIDs(t *testing.T) {
	ackFrames := make(chan map[string]interface{}, 4)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var frame map[string]interface{}
			if json.Unmarshal(raw, &frame) == nil {
				ackFrames <- frame
			}
		}
	}))
	defer server.Close()
	url := "ws" + server.URL[len("http"):]
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := &WebSocketClient{writeCh: make(chan writeRequest, 8), closeCh: make(chan struct{})}
	go client.startWriter()
	legacyPendingMu.Lock()
	legacyPendingMessages = map[string][]callapi.ActionMessage{}
	legacyPendingMu.Unlock()
	path := filepath.Join(t.TempDir(), "protocol-ids.db")
	if err := OpenMessageIDAllocator(path); err != nil {
		t.Fatal(err)
	}
	defer CloseMessageIDAllocator()

	client.recvMessageOn(conn, []byte(`{"action":"send_group_msg","params":{"group_id":22001,"message":"reply"},"echo":{"opaque":[1,true,null]}}`))
	var first map[string]interface{}
	select {
	case first = <-ackFrames:
	case <-time.After(time.Second):
		t.Fatal("send ACK was not written")
	}
	if first["status"] != "ok" || first["retcode"] != float64(0) {
		t.Fatalf("send ACK: %v", first)
	}
	if first["echo"].(map[string]interface{})["opaque"] == nil {
		t.Fatalf("echo JSON was changed: %v", first["echo"])
	}
	firstID := first["data"].(map[string]interface{})["message_id"].(float64)
	if firstID <= 0 || firstID > 2147483647 {
		t.Fatalf("message_id outside positive int32: %v", firstID)
	}

	client.recvMessageOn(conn, []byte(`{"action":"send_msg","params":{"group_id":22001,"user_id":11001,"message":"ambiguous"},"echo":73}`))
	var rejected map[string]interface{}
	select {
	case rejected = <-ackFrames:
	case <-time.After(time.Second):
		t.Fatal("rejected action ACK was not written")
	}
	if rejected["status"] != "failed" || rejected["echo"] != float64(73) {
		t.Fatalf("invalid send ACK: %v", rejected)
	}

	client.recvMessageOn(conn, []byte(`{"action":"send_group_msg","params":{"group_id":22001.5},"echo":{"invalid":"numeric ID"}}`))
	var malformed map[string]interface{}
	select {
	case malformed = <-ackFrames:
	case <-time.After(time.Second):
		t.Fatal("malformed parameters did not receive an ACK")
	}
	if malformed["status"] != "failed" || malformed["echo"].(map[string]interface{})["invalid"] != "numeric ID" {
		t.Fatalf("malformed parameter ACK did not preserve echo: %v", malformed)
	}

	if err := CloseMessageIDAllocator(); err != nil {
		t.Fatal(err)
	}
	if err := OpenMessageIDAllocator(path); err != nil {
		t.Fatal(err)
	}
	client.recvMessageOn(conn, []byte(`{"action":"send_private_msg","params":{"user_id":11001,"message":"next"},"echo":"opaque"}`))
	var second map[string]interface{}
	select {
	case second = <-ackFrames:
	case <-time.After(time.Second):
		t.Fatal("second send ACK was not written")
	}
	secondID := second["data"].(map[string]interface{})["message_id"].(float64)
	if secondID <= firstID {
		t.Fatalf("persisted message IDs were reused: %v then %v", firstID, secondID)
	}
	if second["echo"] != "opaque" {
		t.Fatalf("string echo not preserved: %v", second["echo"])
	}
	_ = client.Close()
}

func TestOneShotWriteFailureIsNotReplayedAfterConnectionReplacement(t *testing.T) {
	firstFrames := make(chan map[string]interface{}, 4)
	firstRelease := make(chan struct{})
	firstUpgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	firstServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := firstUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var frame map[string]interface{}
			if json.Unmarshal(data, &frame) == nil {
				firstFrames <- frame
			}
			select {
			case <-firstRelease:
				return
			default:
			}
		}
	}))
	defer firstServer.Close()
	firstURL := "ws" + firstServer.URL[len("http"):]
	firstConn, _, err := websocket.DefaultDialer.Dial(firstURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	client := &WebSocketClient{conn: firstConn, backendID: "fixture", writeCh: make(chan writeRequest, 8), closeCh: make(chan struct{})}
	go client.startWriter()
	message := map[string]interface{}{"post_type": "message", "message_id": 501, "message": "ONE_SHOT_FIXTURE"}
	if err := client.SendOneShotMessage(message); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-firstFrames:
		if got["message_id"] != float64(501) {
			t.Fatalf("unexpected first frame: %v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("first WebSocket did not receive one-shot event")
	}

	close(firstRelease)
	_ = firstConn.Close()
	deadline := time.Now().Add(time.Second)
	var failed error
	for time.Now().Before(deadline) {
		failed = client.SendOneShotMessage(message)
		if failed != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if failed == nil {
		t.Fatal("write on closed connection unexpectedly succeeded")
	}

	secondFrames := make(chan map[string]interface{}, 4)
	secondUpgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	secondServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := secondUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var frame map[string]interface{}
			if json.Unmarshal(data, &frame) == nil {
				secondFrames <- frame
			}
		}
	}))
	defer secondServer.Close()
	secondURL := "ws" + secondServer.URL[len("http"):]
	secondConn, _, err := websocket.DefaultDialer.Dial(secondURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	client.connMu.Lock()
	client.conn = secondConn
	client.connMu.Unlock()
	defer client.Close()
	if err := client.SendMessage(map[string]interface{}{"post_type": "meta_event", "meta_event_type": "heartbeat"}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-secondFrames:
		if got["meta_event_type"] != "heartbeat" {
			t.Fatalf("failed event replayed after reconnect: %v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("reconnected WebSocket did not receive heartbeat")
	}
	select {
	case got := <-secondFrames:
		t.Fatalf("unexpected replay after reconnect: %v", got)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestSocketRegistrationIDNeverAliasesReplacementConnection(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	url := "ws" + server.URL[len("http"):]
	client := &WebSocketClient{backendID: "fixture", writeCh: make(chan writeRequest, 4), closeCh: make(chan struct{})}
	go client.startWriter()
	first, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	firstID := client.attachSocket(first)
	client.detachSocket(firstID, first)
	_ = first.Close()
	second, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	secondID := client.attachSocket(second)
	if firstID == secondID {
		t.Fatalf("socket ID was reused: %q", firstID)
	}
	if err := client.SendBridgeMessage(t.Context(), firstID, map[string]interface{}{"stale": true}); err == nil {
		t.Fatal("old socket identity resolved to a replacement connection")
	}
	client.detachSocket(secondID, second)
	_ = client.Close()
}
