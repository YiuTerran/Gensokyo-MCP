package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hoshinonyaruko/gensokyo-mcp/bridge"
	"github.com/hoshinonyaruko/gensokyo-mcp/wsclient"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

const (
	httpFixtureMCPToken      = "http-fixture-mcp-token"
	httpFixtureInternalToken = "http-fixture-internal-token"
	httpFixtureBackend       = "sealdice"
)

type httpMCPClient struct {
	origin  string
	session string
	client  *http.Client
}

func (c *httpMCPClient) post(ctx context.Context, body any) (map[string]any, error) {
	encoded, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.origin+"/mcp", bytes.NewReader(encoded))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+httpFixtureMCPToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	if c.session != "" {
		req.Header.Set("Mcp-Session-Id", c.session)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if got := resp.Header.Get("Mcp-Session-Id"); got != "" {
		c.session = got
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("MCP HTTP status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 256*1024+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > 256*1024 {
		return nil, errors.New("MCP response exceeded test limit")
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, nil
	}
	if strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream") {
		var last []byte
		for _, line := range strings.Split(string(raw), "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "data:") {
				last = []byte(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
			}
		}
		if len(last) == 0 {
			return nil, errors.New("MCP SSE response has no data event")
		}
		raw = last
	}
	var result map[string]any
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("decode MCP response: %w", err)
	}
	return result, nil
}

func (c *httpMCPClient) rpc(ctx context.Context, id any, method string, params any) (map[string]any, error) {
	return c.post(ctx, map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
}

func (c *httpMCPClient) notify(ctx context.Context, method string, params any) error {
	_, err := c.post(ctx, map[string]any{"jsonrpc": "2.0", "method": method, "params": params})
	return err
}

func (c *httpMCPClient) initialize(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.rpc(ctx, 1, "initialize", map[string]any{
		"protocolVersion": "2024-11-05",
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]any{"name": "gensokyo-http-fixture", "version": "1"},
	}); err != nil {
		t.Fatalf("initialize MCP session: %v", err)
	}
	if c.session == "" {
		t.Fatal("MCP initialize did not return Mcp-Session-Id")
	}
	if err := c.notify(ctx, "notifications/initialized", map[string]any{}); err != nil {
		t.Fatalf("complete MCP initialization: %v", err)
	}
	listed, err := c.rpc(ctx, 2, "tools/list", map[string]any{})
	if err != nil {
		t.Fatalf("list tools: %v", err)
	}
	result, _ := listed["result"].(map[string]any)
	tools, _ := result["tools"].([]any)
	for _, item := range tools {
		tool, _ := item.(map[string]any)
		if tool["name"] == "call_ws" {
			return
		}
	}
	t.Fatalf("call_ws missing from tools/list: %v", listed)
}

func (c *httpMCPClient) call(ctx context.Context, rpcID any, requestID, payload string) (map[string]any, error) {
	return c.callWithRole(ctx, rpcID, requestID, payload, "group", nil, false)
}

func (c *httpMCPClient) callWithRole(ctx context.Context, rpcID any, requestID, payload, audience string, role any, includeRole bool) (map[string]any, error) {
	arguments := map[string]any{
		"backend_id": httpFixtureBackend,
		"request_id": requestID,
		"audience":   audience,
		"payload":    payload,
		"user_id":    11001,
	}
	if audience == "group" {
		arguments["group_id"] = 22001
	}
	if includeRole {
		arguments["group_role"] = role
	}
	return c.rpc(ctx, rpcID, "tools/call", map[string]any{
		"name":      "call_ws",
		"arguments": arguments,
	})
}

func bridgeResultFromRPC(t *testing.T, response map[string]any) (bridge.Result, bool) {
	t.Helper()
	outer, _ := response["result"].(map[string]any)
	isError, _ := outer["isError"].(bool)
	content, _ := outer["content"].([]any)
	if len(content) != 1 {
		t.Fatalf("expected one MCP content item, got %v", outer)
	}
	item, _ := content[0].(map[string]any)
	text, _ := item["text"].(string)
	var result bridge.Result
	if err := json.Unmarshal([]byte(text), &result); err != nil {
		t.Fatalf("decode bridge tool result %q: %v", text, err)
	}
	return result, isError
}

type fakeBackendAction struct {
	Action string          `json:"action"`
	Params json.RawMessage `json:"params"`
	Echo   json.RawMessage `json:"echo"`
}

type fakeBackendAck struct {
	Status  string          `json:"status"`
	Retcode int             `json:"retcode"`
	Data    json.RawMessage `json:"data"`
	Echo    json.RawMessage `json:"echo"`
}

type fakeBackendEvent struct {
	Payload  string
	SourceID int32
	Role     string
}

type pendingFakeAck struct {
	ack fakeBackendAck
	err error
}

type fakeOneBotBackend struct {
	t            *testing.T
	server       *httptest.Server
	client       *wsclient.WebSocketClient
	connMu       sync.RWMutex
	conn         *websocket.Conn
	writeMu      sync.Mutex
	pendingMu    sync.Mutex
	pending      map[string]chan pendingFakeAck
	seq          atomic.Int64
	connectionID atomic.Value
	events       chan fakeBackendEvent
	stop         chan struct{}
	closeOnce    sync.Once
	badEchoes    atomic.Int32
	accepted     atomic.Int32
}

func newHTTPBridgeFixture(t *testing.T) (*httpMCPClient, *fakeOneBotBackend, *bridge.Manager, func()) {
	t.Helper()
	manager, err := bridge.Open(t.TempDir() + "/bridge.db")
	if err != nil {
		t.Fatal(err)
	}
	manager.ConfigureBackends([]string{httpFixtureBackend})

	oldEnabled, oldMCPToken, oldInternalToken, oldManager := bridgeEnabled, bridgeMCPToken, bridgeInternalToken, wsBridge
	rpcCancelMu.Lock()
	oldRPCEntries := make(map[string]*rpcCancelEntry, len(rpcCancelBySession))
	for key, entry := range rpcCancelBySession {
		oldRPCEntries[key] = entry
	}
	rpcCancelMu.Unlock()
	wsClientsMu.Lock()
	oldClients := wsClients
	oldBackendClients := wsBackendClients
	wsClients = nil
	wsBackendClients = map[string]*wsclient.WebSocketClient{}
	wsClientsMu.Unlock()
	bridgeEnabled = true
	bridgeMCPToken = httpFixtureMCPToken
	bridgeInternalToken = httpFixtureInternalToken
	wsBridge = manager

	backend := &fakeOneBotBackend{
		t: t, pending: make(map[string]chan pendingFakeAck),
		events: make(chan fakeBackendEvent, 16), stop: make(chan struct{}),
	}
	var client *wsclient.WebSocketClient
	var httpServer *httptest.Server
	var cleanupOnce sync.Once
	cleanup := func() {
		cleanupOnce.Do(func() {
			if httpServer != nil {
				httpServer.Close()
			}
			backend.close()
			_ = manager.Close()
			wsclient.SetBridgeManager(nil)
			bridgeEnabled, bridgeMCPToken, bridgeInternalToken, wsBridge = oldEnabled, oldMCPToken, oldInternalToken, oldManager
			wsClientsMu.Lock()
			wsClients, wsBackendClients = oldClients, oldBackendClients
			wsClientsMu.Unlock()
			rpcCancelMu.Lock()
			rpcCancelBySession = oldRPCEntries
			rpcCancelMu.Unlock()
			wsclient.SetBridgeManager(oldManager)
		})
	}
	t.Cleanup(cleanup)
	backend.server = httptest.NewServer(http.HandlerFunc(backend.serveHTTP))
	wsURL := "ws" + strings.TrimPrefix(backend.server.URL, "http")
	client, err = wsclient.NewWebSocketClient(wsURL, 12345, 0, httpFixtureBackend)
	if err != nil {
		t.Fatalf("create production WebSocket client: %v", err)
	}
	backend.client = client
	wsClientsMu.Lock()
	wsBackendClients[httpFixtureBackend] = client
	wsClients = append(wsClients, client)
	wsClientsMu.Unlock()

	core := NewGensokyoServer()
	core.srv.AddTool(mcp.NewTool("http_context_probe"), func(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		session := server.ClientSessionFromContext(ctx)
		sessionID := ""
		if session != nil {
			sessionID = session.SessionID()
		}
		return mcp.NewToolResultText(requestIDContext(ctx) + "|" + sessionID), nil
	})
	httpServer = httptest.NewServer(buildHTTPHandler(core.srv, "/"))
	mcpClient := &httpMCPClient{origin: httpServer.URL, client: &http.Client{Timeout: 20 * time.Second}}
	waitForBackendReady(t, manager)
	return mcpClient, backend, manager, cleanup
}

func waitForBackendReady(t *testing.T, manager *bridge.Manager) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		statuses := manager.Backends()
		if len(statuses) == 1 && statuses[0].Ready && statuses[0].Version == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("fake OneBot backend did not register: %+v", manager.Backends())
}

func (b *fakeOneBotBackend) serveHTTP(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	b.connMu.Lock()
	b.conn = conn
	b.connMu.Unlock()
	defer conn.Close()
	go b.readLoop(conn)
	ack, err := b.sendAction("_llm_bridge_register", map[string]any{
		"version": 1, "backend_instance": "go-http-fixture", "capabilities": []string{"reply", "complete", bridge.CapabilityGroupRoleV1, bridge.CapabilityLogDisplayV1},
	}, json.RawMessage(`{"register":["arbitrary",true,null,7]}`))
	if err != nil || ack.Status != "ok" {
		return
	}
	var registration struct {
		ConnectionID string   `json:"connection_id"`
		Capabilities []string `json:"capabilities"`
	}
	if json.Unmarshal(ack.Data, &registration) != nil || registration.ConnectionID == "" ||
		!bridgeCapabilityPresent(registration.Capabilities, bridge.CapabilityLogDisplayV1) {
		return
	}
	b.connectionID.Store(registration.ConnectionID)
	select {
	case <-b.stop:
		return
	case <-r.Context().Done():
		return
	}
}

func (b *fakeOneBotBackend) readLoop(conn *websocket.Conn) {
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var envelope struct {
			PostType  string      `json:"post_type"`
			Raw       string      `json:"raw_message"`
			MessageID json.Number `json:"message_id"`
			Sender    struct {
				Role string `json:"role"`
			} `json:"sender"`
			Echo    json.RawMessage `json:"echo"`
			Status  string          `json:"status"`
			Retcode int             `json:"retcode"`
			Data    json.RawMessage `json:"data"`
		}
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.UseNumber()
		if decoder.Decode(&envelope) != nil {
			continue
		}
		if envelope.Echo != nil {
			key := string(envelope.Echo)
			b.pendingMu.Lock()
			waiter := b.pending[key]
			delete(b.pending, key)
			b.pendingMu.Unlock()
			if waiter != nil {
				waiter <- pendingFakeAck{ack: fakeBackendAck{Status: envelope.Status, Retcode: envelope.Retcode, Data: envelope.Data, Echo: envelope.Echo}}
			}
			continue
		}
		if envelope.PostType != "message" || envelope.Raw == "" {
			continue
		}
		source, err := strconv.ParseInt(envelope.MessageID.String(), 10, 32)
		if err != nil || source <= 0 || source > math.MaxInt32 {
			continue
		}
		event := fakeBackendEvent{Payload: envelope.Raw, SourceID: int32(source), Role: envelope.Sender.Role}
		select {
		case b.events <- event:
		default:
		}
		go b.respond(event)
	}
}

func (b *fakeOneBotBackend) sendAction(action string, params any, echo json.RawMessage) (fakeBackendAck, error) {
	if len(echo) == 0 {
		echo = json.RawMessage(fmt.Sprintf(`{"ack":%d}`, b.seq.Add(1)))
	}
	key := string(echo)
	waiter := make(chan pendingFakeAck, 1)
	b.pendingMu.Lock()
	if b.pending[key] != nil {
		b.pendingMu.Unlock()
		return fakeBackendAck{}, errors.New("duplicate fake ACK echo")
	}
	b.pending[key] = waiter
	b.pendingMu.Unlock()
	frame, err := json.Marshal(map[string]any{"action": action, "params": params, "echo": echo})
	if err != nil {
		return fakeBackendAck{}, err
	}
	b.connMu.RLock()
	conn := b.conn
	b.connMu.RUnlock()
	if conn == nil {
		return fakeBackendAck{}, errors.New("fake backend socket unavailable")
	}
	b.writeMu.Lock()
	err = conn.WriteMessage(websocket.TextMessage, frame)
	b.writeMu.Unlock()
	if err != nil {
		b.pendingMu.Lock()
		delete(b.pending, key)
		b.pendingMu.Unlock()
		return fakeBackendAck{}, err
	}
	select {
	case got := <-waiter:
		if got.err != nil {
			return fakeBackendAck{}, got.err
		}
		if !bytes.Equal(bytes.TrimSpace(got.ack.Echo), bytes.TrimSpace(echo)) {
			b.badEchoes.Add(1)
			return got.ack, fmt.Errorf("ACK echo changed: got %s want %s", got.ack.Echo, echo)
		}
		return got.ack, nil
	case <-b.stop:
		return fakeBackendAck{}, errors.New("fake backend stopped")
	case <-time.After(3 * time.Second):
		b.pendingMu.Lock()
		delete(b.pending, key)
		b.pendingMu.Unlock()
		return fakeBackendAck{}, errors.New("timed out waiting for OneBot ACK")
	}
}

func (b *fakeOneBotBackend) respond(event fakeBackendEvent) {
	switch event.Payload {
	case "hold-disconnect", "hold-cancel", "hold-cross-session", "hold-duplicate":
		return
	case "split-late":
		if !b.sendReply(event, "late-first ") || !b.sendReply(event, "late-second") {
			return
		}
		b.sendComplete(event, "ok", 2)
	case "fresh-after-late":
		if b.sendReply(event, "fresh-only") {
			b.sendComplete(event, "ok", 1)
		}
	case "no-output":
		b.sendComplete(event, "ok", 0)
	case "failed-completion":
		b.sendComplete(event, "failed", 0)
	default:
		if b.sendReply(event, "ordinary-output") {
			b.sendComplete(event, "ok", 1)
		}
	}
}

func (b *fakeOneBotBackend) sendReply(event fakeBackendEvent, text string) bool {
	ack, err := b.sendAction("send_group_msg", map[string]any{
		"group_id": 22001,
		"message": []any{
			map[string]any{"type": "reply", "data": map[string]any{"id": event.SourceID}},
			map[string]any{"type": "text", "data": map[string]any{"text": text}},
		},
	}, json.RawMessage(fmt.Sprintf(`{"arbitrary":[%d,true,null,"echo"]}`, b.seq.Add(1))))
	if err != nil || ack.Status != "ok" || ack.Retcode != 0 {
		return false
	}
	b.accepted.Add(1)
	return true
}

func (b *fakeOneBotBackend) sendComplete(event fakeBackendEvent, status string, count int) {
	connectionID, _ := b.connectionID.Load().(string)
	_, _ = b.sendAction("_llm_bridge_complete", map[string]any{
		"version": 1, "source_message_id": event.SourceID,
		"connection_id": connectionID, "status": status, "output_count": count,
	}, json.RawMessage(fmt.Sprintf(`{"complete":"%d"}`, b.seq.Add(1))))
}

func (b *fakeOneBotBackend) waitEvent(t *testing.T, payload string) fakeBackendEvent {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case event := <-b.events:
			if event.Payload == payload {
				if event.SourceID <= 0 || event.SourceID > math.MaxInt32 {
					t.Fatalf("source ID is outside int32: %d", event.SourceID)
				}
				return event
			}
		case <-deadline:
			t.Fatalf("backend did not receive payload %q", payload)
		}
	}
}

func (b *fakeOneBotBackend) sendUnsupportedAndCheckEcho(t *testing.T) {
	t.Helper()
	echo := json.RawMessage(`{"fixture":{"arbitrary":[1,true,null]}}`)
	ack, err := b.sendAction("send_not_supported", map[string]any{}, echo)
	if err != nil {
		t.Fatal(err)
	}
	if ack.Status != "failed" || ack.Retcode == 0 || !bytes.Equal(bytes.TrimSpace(ack.Echo), bytes.TrimSpace(echo)) {
		t.Fatalf("unsupported action ACK mismatch: %+v", ack)
	}
}

func (b *fakeOneBotBackend) close() {
	b.closeOnce.Do(func() {
		close(b.stop)
		if b.client != nil {
			_ = b.client.Close()
		}
		b.connMu.RLock()
		conn := b.conn
		b.connMu.RUnlock()
		if conn != nil {
			_ = conn.Close()
		}
		if b.server != nil {
			b.server.Close()
		}
	})
}

func TestProductionHTTPMCPOneBotBridgeEndToEnd(t *testing.T) {
	mcpClient, backend, _, cleanup := newHTTPBridgeFixture(t)
	defer cleanup()
	mcpClient.initialize(t)

	backend.sendUnsupportedAndCheckEcho(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	first, err := mcpClient.call(ctx, 10, "split-late-request", "split-late")
	if err != nil {
		t.Fatal(err)
	}
	result, toolError := bridgeResultFromRPC(t, first)
	if toolError || result.Status != "ok" || len(result.Outputs) != 2 || result.Outputs[0].Message != "late-first " || result.Outputs[1].Message != "late-second" {
		t.Fatalf("split late completion did not preserve both correlated fragments: %+v; response=%v", result, first)
	}
	second, err := mcpClient.call(ctx, 11, "fresh-request", "fresh-after-late")
	if err != nil {
		t.Fatal(err)
	}
	fresh, toolError := bridgeResultFromRPC(t, second)
	if toolError || fresh.Status != "ok" || len(fresh.Outputs) != 1 || fresh.Outputs[0].Message != "fresh-only" {
		t.Fatalf("previous request output leaked into fresh request: %+v; response=%v", fresh, second)
	}
	for _, test := range []struct {
		payload string
		status  string
	}{
		{payload: "no-output", status: "ok"},
		{payload: "failed-completion", status: "failed"},
	} {
		response, err := mcpClient.call(ctx, test.payload, "request-"+test.payload, test.payload)
		if err != nil {
			t.Fatalf("%s tool call: %v", test.payload, err)
		}
		got, toolError := bridgeResultFromRPC(t, response)
		if toolError || got.Status != test.status || len(got.Outputs) != 0 {
			t.Fatalf("%s completion: %+v; response=%v", test.payload, got, response)
		}
	}
	if backend.badEchoes.Load() != 0 || backend.accepted.Load() != 3 {
		t.Fatalf("OneBot ACK/correlation fixture mismatch: changed echoes=%d accepted outputs=%d", backend.badEchoes.Load(), backend.accepted.Load())
	}
}

func TestProductionHTTPBridgePassesAndValidatesGroupRole(t *testing.T) {
	mcpClient, backend, manager, cleanup := newHTTPBridgeFixture(t)
	defer cleanup()
	mcpClient.initialize(t)
	statuses := manager.Backends()
	if len(statuses) != 1 || !bridgeCapabilityPresent(statuses[0].Capabilities, bridge.CapabilityGroupRoleV1) {
		t.Fatalf("backend did not negotiate group-role-v1: %+v", statuses)
	}

	list, err := mcpClient.rpc(context.Background(), 19, "tools/list", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	listResult, _ := list["result"].(map[string]any)
	tools, _ := listResult["tools"].([]any)
	var roleSchemaFound bool
	for _, item := range tools {
		tool, _ := item.(map[string]any)
		if tool["name"] != "call_ws" {
			continue
		}
		schema, _ := tool["inputSchema"].(map[string]any)
		properties, _ := schema["properties"].(map[string]any)
		_, roleSchemaFound = properties["group_role"]
	}
	if !roleSchemaFound {
		t.Fatalf("call_ws schema omitted the trusted group_role field: %v", list)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	response, err := mcpClient.callWithRole(ctx, 20, "role-event", "role-event", "group", "admin", true)
	if err != nil {
		t.Fatal(err)
	}
	result, toolError := bridgeResultFromRPC(t, response)
	if toolError || result.Status != "ok" {
		t.Fatalf("valid group role call failed: %+v; %v", result, response)
	}
	event := backend.waitEvent(t, "role-event")
	if event.Role != "admin" {
		t.Fatalf("sender.role did not preserve the group role: %q", event.Role)
	}
	unknownResponse, err := mcpClient.callWithRole(ctx, 21, "unknown-role-event", "unknown-role-event", "group", "", true)
	if err != nil {
		t.Fatal(err)
	}
	unknownResult, unknownToolError := bridgeResultFromRPC(t, unknownResponse)
	if unknownToolError || unknownResult.Status != "ok" {
		t.Fatalf("empty unknown group role was rejected: %+v; %v", unknownResult, unknownResponse)
	}
	unknownEvent := backend.waitEvent(t, "unknown-role-event")
	if unknownEvent.Role != "" {
		t.Fatalf("unknown role was defaulted in sender.role: %q", unknownEvent.Role)
	}

	for _, test := range []struct {
		name, audience string
		role           any
	}{
		{name: "invalid role", audience: "group", role: "moderator"},
		{name: "null role", audience: "group", role: nil},
		{name: "private role", audience: "private", role: "admin"},
		{name: "private empty role", audience: "private", role: ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			failed, callErr := mcpClient.callWithRole(ctx, test.name, "reject-"+test.name, test.name, test.audience, test.role, true)
			if callErr != nil {
				t.Fatal(callErr)
			}
			got, isError := bridgeResultFromRPC(t, failed)
			if isError || got.Status != "failed" || len(got.Outputs) != 0 {
				t.Fatalf("invalid role request was not rejected before dispatch: %+v; %v", got, failed)
			}
		})
	}
}

func bridgeCapabilityPresent(capabilities []string, want string) bool {
	for _, capability := range capabilities {
		if capability == want {
			return true
		}
	}
	return false
}

func TestProductionHTTPMCPDisconnectAndSessionCancellationQuarantine(t *testing.T) {
	t.Run("HTTP disconnect", func(t *testing.T) {
		mcpClient, backend, manager, cleanup := newHTTPBridgeFixture(t)
		defer cleanup()
		mcpClient.initialize(t)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := mcpClient.call(ctx, 77, "disconnect-request", "hold-disconnect")
			done <- err
		}()
		backend.waitEvent(t, "hold-disconnect")
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("HTTP request did not disconnect promptly")
		}
		waitForBackendQuarantine(t, manager)
		ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel2()
		response, err := mcpClient.call(ctx2, 78, "disconnect-request", "hold-disconnect")
		if err != nil {
			t.Fatal(err)
		}
		result, toolError := bridgeResultFromRPC(t, response)
		if toolError || result.Status != "unknown" {
			t.Fatalf("disconnected dispatched request must remain unknown: %+v; %v", result, response)
		}
	})

	t.Run("session-scoped notification and duplicate RPC", func(t *testing.T) {
		mcpClient, backend, manager, cleanup := newHTTPBridgeFixture(t)
		defer cleanup()
		mcpClient.initialize(t)
		probe, err := mcpClient.rpc(context.Background(), 42, "tools/call", map[string]any{
			"name": "http_context_probe", "arguments": map[string]any{},
		})
		if err != nil {
			t.Fatal(err)
		}
		probeResult, _ := probe["result"].(map[string]any)
		probeContent, _ := probeResult["content"].([]any)
		probeItem, _ := probeContent[0].(map[string]any)
		if got, _ := probeItem["text"].(string); got != "42|"+mcpClient.session {
			t.Fatalf("HTTP context lost RPC/session values before tool handler: got %q want %q", got, "42|"+mcpClient.session)
		}
		otherSession := &httpMCPClient{origin: mcpClient.origin, client: mcpClient.client}
		otherSession.initialize(t)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		firstDone := make(chan error, 1)
		go func() {
			_, err := mcpClient.call(ctx, 99, "cross-session-request", "hold-cross-session")
			firstDone <- err
		}()
		backend.waitEvent(t, "hold-cross-session")
		key := mcpClient.session + "\x00" + "99"
		rpcCancelMu.Lock()
		_, registeredCall := rpcCancelBySession[key]
		activeKeys := make([]string, 0, len(rpcCancelBySession))
		for key := range rpcCancelBySession {
			activeKeys = append(activeKeys, key)
		}
		rpcCancelMu.Unlock()
		if !registeredCall {
			t.Fatalf("active call was not keyed by its MCP session and RPC id %q; active keys: %q", key, activeKeys)
		}

		// An identical JSON-RPC id in a different MCP session must not cancel
		// the running call, whose cancellation key includes the session header.
		if err := otherSession.notify(context.Background(), "notifications/cancelled", map[string]any{"requestId": 99}); err != nil {
			t.Fatalf("foreign-session cancellation notification: %v", err)
		}
		if !manager.Backends()[0].Ready {
			t.Fatal("same RPC ID from another session canceled or quarantined the active request")
		}

		duplicate, err := mcpClient.call(context.Background(), 99, "duplicate-other-request", "ordinary-after-duplicate")
		if err != nil {
			t.Fatalf("duplicate active RPC response: %v", err)
		}
		if outer, _ := duplicate["result"].(map[string]any); outer["isError"] != true {
			t.Fatalf("duplicate active RPC was not rejected: %v", duplicate)
		}

		if err := mcpClient.notify(context.Background(), "notifications/cancelled", map[string]any{"requestId": 99}); err != nil {
			t.Fatalf("same-session cancellation notification: %v", err)
		}
		cancel()
		select {
		case <-firstDone:
		case <-time.After(3 * time.Second):
			t.Fatal("same-session cancellation did not end the HTTP request")
		}
		waitForBackendQuarantine(t, manager)
		ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel2()
		response, err := mcpClient.call(ctx2, 100, "cross-session-request", "hold-cross-session")
		if err != nil {
			t.Fatal(err)
		}
		result, toolError := bridgeResultFromRPC(t, response)
		if toolError || result.Status != "unknown" {
			t.Fatalf("canceled dispatched request must be unknown: %+v; %v", result, response)
		}
	})
}

func waitForBackendQuarantine(t *testing.T, manager *bridge.Manager) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		statuses := manager.Backends()
		if len(statuses) == 1 && !statuses[0].Ready {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("dispatched cancellation did not quarantine the backend: %+v", manager.Backends())
}
