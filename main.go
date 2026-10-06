// gensokyo-mcp/main.go
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hoshinonyaruko/gensokyo-mcp/Processor"
	"github.com/hoshinonyaruko/gensokyo-mcp/botstats"
	"github.com/hoshinonyaruko/gensokyo-mcp/bridge"
	"github.com/hoshinonyaruko/gensokyo-mcp/callapi"
	"github.com/hoshinonyaruko/gensokyo-mcp/config"
	"github.com/hoshinonyaruko/gensokyo-mcp/praser"
	"github.com/hoshinonyaruko/gensokyo-mcp/sys"
	"github.com/hoshinonyaruko/gensokyo-mcp/template"
	"github.com/hoshinonyaruko/gensokyo-mcp/wsclient"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"gopkg.in/fsnotify.v1"
)

// 消息处理器，持有 openapi 对象
var wsClients []*wsclient.WebSocketClient
var wsBackendClients = map[string]*wsclient.WebSocketClient{}
var wsClientsMu sync.RWMutex
var wsBridge *bridge.Manager
var bridgeEnabled bool
var bridgeMCPToken string
var bridgeInternalToken string

// ---------- Context helpers ----------

type bearerKey struct{}
type rpcIDContextKey struct{}

var rpcCancelMu sync.Mutex

type rpcCancelEntry struct{ cancel context.CancelFunc }

var rpcCancelBySession = map[string]*rpcCancelEntry{}

func withBearer(ctx context.Context, b string) context.Context {
	return context.WithValue(ctx, bearerKey{}, b)
}

func bearerFromRequest(ctx context.Context, r *http.Request) context.Context {
	return withBearer(ctx, r.Header.Get("Authorization"))
}

func bearerFromEnv(ctx context.Context) context.Context {
	return withBearer(ctx, os.Getenv("BEARER"))
}

func requestIDContext(ctx context.Context) string {
	value, _ := ctx.Value(rpcIDContextKey{}).(string)
	return value
}

func withRPCRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Body == nil {
			next.ServeHTTP(w, r)
			return
		}
		const maxMCPBody = 1024 * 1024
		body, err := io.ReadAll(io.LimitReader(r.Body, maxMCPBody+1))
		_ = r.Body.Close()
		if err != nil || len(body) > maxMCPBody {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		r.Body = io.NopCloser(strings.NewReader(string(body)))
		var envelope struct {
			ID json.RawMessage `json:"id"`
		}
		if json.Unmarshal(body, &envelope) == nil && len(envelope.ID) != 0 && string(envelope.ID) != "null" {
			ctx := context.WithValue(r.Context(), rpcIDContextKey{}, strings.TrimSpace(string(envelope.ID)))
			r = r.WithContext(ctx)
		}
		next.ServeHTTP(w, r)
	})
}

func registerRPCCancel(ctx context.Context, cancel context.CancelFunc) (func(), bool) {
	rpcID := requestIDContext(ctx)
	session := server.ClientSessionFromContext(ctx)
	if rpcID == "" || session == nil || session.SessionID() == "" {
		return func() {}, true
	}
	key := session.SessionID() + "\x00" + rpcID
	entry := &rpcCancelEntry{cancel: cancel}
	rpcCancelMu.Lock()
	if rpcCancelBySession[key] != nil {
		rpcCancelMu.Unlock()
		return func() {}, false
	}
	rpcCancelBySession[key] = entry
	rpcCancelMu.Unlock()
	return func() {
		rpcCancelMu.Lock()
		if rpcCancelBySession[key] == entry {
			delete(rpcCancelBySession, key)
		}
		rpcCancelMu.Unlock()
	}, true
}

const (
	defaultBridgeSelfID int64 = 10000
	maxSafeOneBotID     int64 = 1<<53 - 1
)

func applyOneBotEnvironment(conf *config.Config) error {
	wsURL := os.Getenv("ONEBOT_WS_URL")
	backendID := os.Getenv("ONEBOT_BACKEND_ID")
	token := os.Getenv("ONEBOT_WS_TOKEN")
	if bridgeEnabled {
		if conf.Settings.Uin < 0 || conf.Settings.Uin > maxSafeOneBotID {
			return errors.New("configured OneBot self ID must be a positive safe integer")
		}
		if rawSelfID := os.Getenv("ONEBOT_SELF_ID"); rawSelfID != "" {
			selfID, err := parsePositiveSafeOneBotID(rawSelfID)
			if err != nil {
				return err
			}
			conf.Settings.Uin = selfID
		} else if conf.Settings.Uin == 0 {
			conf.Settings.Uin = defaultBridgeSelfID
		}
		if conf.Settings.Uin <= 0 {
			return errors.New("bridge mode requires a positive OneBot self ID")
		}
	}
	if bridgeEnabled && wsURL == "" && (len(conf.Settings.WsAddress) == 0 || strings.Contains(strings.Join(conf.Settings.WsAddress, ""), "<YOUR_WS_ADDRESS>")) {
		wsURL = "ws://sealdice:18081/ws"
	}
	if wsURL != "" {
		if backendID == "" {
			backendID = "sealdice"
		}
		conf.Settings.WsAddress = []string{wsURL}
		conf.Settings.WsBackendID = []string{backendID}
		// An override URL never inherits an unrelated token from config.yml.
		conf.Settings.WsToken = []string{token}
	} else if backendID != "" && len(conf.Settings.WsAddress) == 1 {
		conf.Settings.WsBackendID = []string{backendID}
	}
	if token != "" && wsURL == "" && len(conf.Settings.WsAddress) == 1 {
		conf.Settings.WsToken = []string{token}
	}
	return nil
}

func parsePositiveSafeOneBotID(raw string) (int64, error) {
	if raw == "" {
		return 0, errors.New("OneBot self ID is empty")
	}
	for _, char := range raw {
		if char < '0' || char > '9' {
			return 0, errors.New("OneBot self ID must be a positive safe integer")
		}
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 || value > maxSafeOneBotID || strconv.FormatInt(value, 10) != raw {
		return 0, errors.New("OneBot self ID must be a positive safe integer")
	}
	return value, nil
}

// ---------- WebSocket tool handler ----------

// ---------- MCP server wrapper ----------

type GensokyoServer struct {
	srv *server.MCPServer
}

func NewGensokyoServer() *GensokyoServer {
	wsclient.SetBridgeManager(wsBridge)
	hooks := &server.Hooks{}
	hooks.AddAfterListTools(func(_ context.Context, _ any, _ *mcp.ListToolsRequest, result *mcp.ListToolsResult) {
		if !bridgeEnabled || bridgeReadyBackendAvailable() {
			return
		}
		filtered := result.Tools[:0]
		for _, tool := range result.Tools {
			if tool.Name != "call_ws" {
				filtered = append(filtered, tool)
			}
		}
		result.Tools = filtered
	})
	s := server.NewMCPServer(
		"gensokyo-mcp",
		"0.1.0",
		server.WithResourceCapabilities(true, true),
		server.WithToolCapabilities(true),
		server.WithHooks(hooks),
	)
	s.AddNotificationHandler("notifications/cancelled", func(ctx context.Context, notification mcp.JSONRPCNotification) {
		session := server.ClientSessionFromContext(ctx)
		if session == nil || session.SessionID() == "" {
			return
		}
		value, ok := notification.Params.AdditionalFields["requestId"]
		if !ok {
			return
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return
		}
		key := session.SessionID() + "\x00" + string(encoded)
		rpcCancelMu.Lock()
		entry := rpcCancelBySession[key]
		rpcCancelMu.Unlock()
		if entry != nil {
			entry.cancel()
		}
	})

	var wsTool mcp.Tool
	if bridgeEnabled {
		wsTool = mcp.NewTool("call_ws",
			mcp.WithDescription("Send one text command to a registered OneBot backend and collect only correlated replies before explicit completion."),
			mcp.WithString("payload", mcp.Description("One-line text command, up to 4000 characters")),
			mcp.WithString("backend_id", mcp.Description("Configured backend alias")),
			mcp.WithString("request_id", mcp.Description("Trusted idempotency key")),
			mcp.WithString("audience", mcp.Description("group or private")),
			mcp.WithString("user_key", mcp.Description("Trusted opaque user identity")),
			mcp.WithString("group_key", mcp.Description("Trusted opaque group identity")),
			mcp.WithString("group_role", mcp.Description("Trusted current-group role snapshot: owner, admin, or member; omit or pass an empty string when unknown")),
			mcp.WithNumber("user_id", mcp.Description("Compatibility OneBot user ID: a safe positive integer or canonical decimal string")),
			mcp.WithNumber("group_id", mcp.Description("Compatibility OneBot group ID: a safe positive integer or canonical decimal string")),
		)
		wsTool.InputSchema.Properties["user_id"] = compatOneBotIDSchema("Compatibility OneBot user ID")
		wsTool.InputSchema.Properties["group_id"] = compatOneBotIDSchema("Compatibility OneBot group ID")
	} else {
		// Keep the original tool schema when the optional bridge is disabled.
		wsTool = mcp.NewTool("call_ws",
			mcp.WithDescription("连接目标 Onebot Ws 调用bot并取得回复."),
			mcp.WithString("payload", mcp.Description("可选：发送到服务器的文本负载"), mcp.DefaultString("帮助")),
			mcp.WithString("user_id", mcp.Description("可选：测试使用的user_id"), mcp.DefaultString("0")),
			mcp.WithString("group_id", mcp.Description("可选：测试使用的group_id"), mcp.DefaultString("0")),
			mcp.WithNumber("timeout", mcp.Description("连接与首条消息读取超时，单位秒，默认 10"), mcp.DefaultNumber(10), mcp.Min(1)),
		)
	}

	// 可以add 多个tool
	s.AddTool(wsTool, callWS)
	result := &GensokyoServer{srv: s}
	if wsBridge != nil {
		wsBridge.SetReadinessCallback(func() { s.SendNotificationToAllClients(mcp.MethodNotificationToolsListChanged, nil) })
	}
	return result
}

func bridgeReadyBackendAvailable() bool {
	if wsBridge == nil {
		return false
	}
	for _, backend := range wsBridge.Backends() {
		if backend.Ready && backend.Version == 1 {
			return true
		}
	}
	return false
}

func (g *GensokyoServer) HTTPServer() *server.StreamableHTTPServer {
	return server.NewStreamableHTTPServer(
		g.srv,
		server.WithHTTPContextFunc(bearerFromRequest), // 把 Authorization 注入 ctx
	)
}

func (g *GensokyoServer) ServeStdio() error {
	return server.ServeStdio(
		g.srv,
		server.WithStdioContextFunc(bearerFromEnv), // 从环境变量注入 ctx
	)
}

// ---------- main ------------

func main() {
	transport := flag.String("t", "http", "Transport: http | stdio")
	addr := flag.String("addr", ":8090", "HTTP listen address")
	flag.Parse()
	bridgeEnabled = strings.EqualFold(os.Getenv("LLM_BRIDGE_ENABLED"), "true") || os.Getenv("LLM_BRIDGE_ENABLED") == "1"
	bridgeMCPToken = os.Getenv("LLM_BRIDGE_MCP_TOKEN")
	bridgeInternalToken = os.Getenv("LLM_BRIDGE_INTERNAL_TOKEN")

	if _, err := os.Stat("config.yml"); os.IsNotExist(err) {
		var err error

		// 将 <YOUR_SERVER_DIR> 替换成实际的内网IP地址 确保初始状态webui能够被访问
		configData := template.ConfigTemplate

		// 将修改后的配置写入 config.yml
		err = os.WriteFile("config.yml", []byte(configData), 0644)
		if err != nil {
			log.Println("Error writing config.yml:", err)
			return
		}

		if !bridgeEnabled {
			log.Println("请配置config.yml然后再次运行.")
			log.Print("按下 Enter 继续...")
			bufio.NewReader(os.Stdin).ReadBytes('\n')
			os.Exit(0)
		}
	}

	// 主逻辑
	// 加载配置
	conf, err := config.LoadConfig("config.yml", false)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	if err := applyOneBotEnvironment(conf); err != nil {
		log.Fatal("invalid OneBot bridge account identity")
	}
	if bridgeEnabled {
		if len(conf.Settings.WsBackendID) != len(conf.Settings.WsAddress) {
			log.Fatal("bridge mode requires one explicit backend ID for each OneBot URL")
		}
		for i, address := range conf.Settings.WsAddress {
			if address != "" && conf.Settings.WsBackendID[i] == "" {
				log.Fatal("bridge mode requires one explicit backend ID for each OneBot URL")
			}
		}
	}
	backendIDs, err := resolveBackendIDs(conf.Settings.WsAddress, conf.Settings.WsBackendID)
	if err != nil {
		log.Fatalf("invalid WebSocket backend configuration")
	}
	if bridgeEnabled {
		if bridgeMCPToken == "" || bridgeInternalToken == "" {
			log.Fatal("bridge MCP and internal tokens are required")
		}
		dataDir := os.Getenv("LLM_BRIDGE_DATA_DIR")
		if dataDir == "" {
			log.Fatal("LLM_BRIDGE_DATA_DIR is required when bridge is enabled")
		}
		if err := os.MkdirAll(dataDir, 0700); err != nil {
			log.Fatal("bridge data directory is unavailable")
		}
		wsBridge, err = bridge.Open(filepath.Join(dataDir, "bridge.db"))
		if err != nil {
			log.Fatal("bridge state store could not be opened")
		}
		defer wsBridge.Close()
		masterUserKeys, valid := bridge.ParseMasterUserKeys(os.Getenv("LLM_BRIDGE_MASTER_USER_KEYS"))
		if !valid {
			// Never include configured identity keys in diagnostics.
			log.Print("LLM_BRIDGE_MASTER_USER_KEYS is invalid; bridge Master ACL is disabled")
		}
		wsBridge.SetMasterUserKeys(masterUserKeys)
		wsBridge.ConfigureBackends(backendIDs)
	} else if err := wsclient.OpenMessageIDAllocator(filepath.Join(".", "message_ids.db")); err != nil {
		log.Fatal("OneBot message ID allocator could not be opened")
	} else {
		defer wsclient.CloseMessageIDAllocator()
	}
	wsclient.SetBridgeManager(wsBridge)
	s := NewGensokyoServer()

	// 配置热重载
	go setupConfigWatcher("config.yml")

	//创建botstats数据库
	botstats.InitializeDB()

	sys.SetTitle(conf.Settings.Title)

	// 启动多个WebSocket客户端的逻辑
	if !allEmpty(conf.Settings.WsAddress) {
		type wsClientResult struct {
			backendID string
			client    *wsclient.WebSocketClient
		}
		wsClientChan := make(chan wsClientResult, len(conf.Settings.WsAddress))
		errorChan := make(chan error, len(conf.Settings.WsAddress))
		// 定义计数器跟踪尝试建立的连接数
		attemptedConnections := 0
		for i, wsAddr := range conf.Settings.WsAddress {
			if wsAddr == "" {
				continue // Skip empty addresses
			}
			attemptedConnections++ // 增加尝试连接的计数
			backendID := backendIDs[i]
			go func(address, id string) {
				retry := config.GetLaunchReconectTimes()
				BotID := uint64(config.GetUinint64())
				wsClient, err := wsclient.NewWebSocketClient(address, BotID, retry, id)
				if err != nil {
					log.Printf("WebSocket connection failed for backend %s", id)
					errorChan <- err
					return
				}
				wsClientChan <- wsClientResult{backendID: id, client: wsClient}
			}(wsAddr, backendID)
		}
		// 获取连接成功后的wsClient
		for i := 0; i < attemptedConnections; i++ {
			select {
			case result := <-wsClientChan:
				wsClientsMu.Lock()
				wsClients = append(wsClients, result.client)
				wsBackendClients[result.backendID] = result.client
				wsClientsMu.Unlock()
			case err := <-errorChan:
				_ = err
				log.Print("OneBot backend failed to connect during startup")
			}
		}

		// 确保所有尝试建立的连接都有对应的wsClient
		if len(wsClients) == 0 {
			log.Println("Error: Not all wsClients are initialized!(反向ws未设置或全部连接失败)")
			// 处理连接失败的情况 只启动正向
			//p = Processor.NewProcessorV2(&conf.Settings)
		} else {
			log.Println("All wsClients are successfully initialized.")
			// 所有客户端都成功初始化
			//p = Processor.NewProcessor(&conf.Settings, wsClients)
		}
	} else {
		// p一定需要初始化
		//p = Processor.NewProcessorV2(&conf.Settings)
		// 如果只启动了http api
		if !conf.Settings.EnableWsServer {
			if conf.Settings.HttpAddress != "" {
				// 对全局生效
				conf.Settings.HttpOnlyBot = true
				log.Println("提示,目前只启动了httpapi,正反向ws均未配置.")
			} else {
				log.Println("提示,目前你配置了个寂寞,httpapi没设置,正反ws都没配置.")
			}
		} else {
			if conf.Settings.HttpAddress != "" {
				log.Println("提示,目前启动了正向ws和httpapi,未连接反向ws")
			} else {
				log.Println("提示,目前启动了正向ws,未连接反向ws,httpapi未开启")
			}
		}
	}
	defer func() {
		wsClientsMu.RLock()
		clients := append([]*wsclient.WebSocketClient(nil), wsClients...)
		wsClientsMu.RUnlock()
		for _, client := range clients {
			if client != nil {
				_ = client.Close()
			}
		}
	}()

	switch *transport {
	case "stdio":
		if err := s.ServeStdio(); err != nil {
			log.Fatalf("stdio server: %v", err)
		}
	case "http":
		serveHTTP(context.TODO(), s.srv, *addr)
	default:
		log.Fatalf("unknown transport: %s", *transport)
	}

}

// ---------- 启动 HTTP 服务器：/mcp → Streamable HTTP  /sse → 旧式 SSE ----------
func serveHTTP(ctx context.Context, core *server.MCPServer, addr string) error {
	httpSrv := &http.Server{Addr: addr, Handler: buildHTTPHandler(core, addr)}

	// 异步启动
	errCh := make(chan error, 1)
	go func() {
		baseURL := httpBaseURL(addr)
		log.Printf("🚀 Streamable HTTP → %s/mcp\n", baseURL)
		log.Printf("🚀 SSE            → %s/sse\n", baseURL)
		errCh <- httpSrv.ListenAndServe()
	}()

	// 监听系统信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		log.Printf("🛑 got %v, shutting down...", sig)
		_ = httpSrv.Close()
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	return nil
}

func buildHTTPHandler(core *server.MCPServer, address ...string) http.Handler {
	baseURL := "http://127.0.0.1"
	if len(address) > 0 && address[0] != "" {
		baseURL = httpBaseURL(address[0])
	}
	streamSrv := server.NewStreamableHTTPServer(
		core,
		server.WithHTTPContextFunc(bearerFromRequest), // 注入 bearer
	)

	sseSrv := server.NewSSEServer(
		core,
		server.WithStaticBasePath("/sse"), // 旧客户端连 GET /sse 拿 schema
		server.WithBaseURL(baseURL),       // 生成绝对路径
		server.WithSSEContextFunc(bearerFromRequest), // 同样注入 bearer
	)

	mux := http.NewServeMux()
	mcpRoutes := withRPCRequestID(streamSrv)
	sseRoutes := withRPCRequestID(sseSrv)
	if bridgeEnabled {
		mcpRoutes = bridge.MCPAuth(bridgeMCPToken, mcpRoutes)
		sseRoutes = bridge.MCPAuth(bridgeMCPToken, sseRoutes)
	}
	mux.Handle("/mcp", mcpRoutes)
	mux.Handle("/sse/", sseRoutes)
	redirect := http.Handler(http.RedirectHandler("/sse/", http.StatusMovedPermanently))
	if bridgeEnabled {
		redirect = bridge.MCPAuth(bridgeMCPToken, redirect)
	}
	mux.Handle("/sse", redirect)
	mux.Handle("/healthz", bridge.HealthHandler())
	if bridgeEnabled {
		mux.Handle("/internal/", bridge.InternalHandler(wsBridge, bridgeInternalToken))
	}

	return mux
}

func httpBaseURL(address string) string {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return "http://127.0.0.1"
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port)
}

func setupConfigWatcher(configFilePath string) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatalf("Error setting up watcher: %v", err)
	}

	// 添加一个100毫秒的Debouncing
	//fileLoader := &config.ConfigFileLoader{EventDelay: 100 * time.Millisecond}

	// Start the goroutine to handle file system events.
	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return // Exit if channel is closed.
				}
				if event.Op&fsnotify.Write == fsnotify.Write {
					fmt.Println("检测到配置文件变动:", event.Name)
					//fileLoader.LoadConfigF(configFilePath)
					config.LoadConfig(configFilePath, true)
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return // Exit if channel is closed.
				}
				log.Println("Watcher error:", err)
			}
		}
	}()

	// Add the config file to the list of watched files.
	err = watcher.Add(configFilePath)
	if err != nil {
		log.Fatalf("Error adding watcher: %v", err)
	}
}

// allEmpty checks if all the strings in the slice are empty.
func allEmpty(addresses []string) bool {
	for _, addr := range addresses {
		if addr != "" {
			return false
		}
	}
	return true
}

func resolveBackendIDs(addresses, configured []string) ([]string, error) {
	active := 0
	for _, address := range addresses {
		if address != "" {
			active++
		}
	}
	if active == 0 {
		return nil, nil
	}
	ids := append([]string(nil), configured...)
	if len(ids) == 1 && ids[0] == "default" && active > 1 {
		ids = make([]string, len(addresses))
		for i := range ids {
			ids[i] = fmt.Sprintf("backend-%d", i+1)
		}
	}
	if len(ids) == 0 {
		if active == 1 {
			ids = make([]string, len(addresses))
			for i, address := range addresses {
				if address != "" {
					ids[i] = "default"
				}
			}
		} else {
			return nil, fmt.Errorf("configure one ws_backend_id for each ws_address")
		}
	}
	if len(ids) != len(addresses) {
		return nil, fmt.Errorf("got %d ws_backend_id values for %d ws_address values", len(ids), len(addresses))
	}
	seen := map[string]bool{}
	for i, address := range addresses {
		if address == "" {
			continue
		}
		if ids[i] == "" {
			return nil, fmt.Errorf("ws_backend_id[%d] is empty", i)
		}
		if seen[ids[i]] {
			return nil, fmt.Errorf("duplicate backend id %q", ids[i])
		}
		seen[ids[i]] = true
	}
	return ids, nil
}

func callWS(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if !bridgeEnabled || wsBridge == nil {
		return callWSLegacy(ctx, req)
	}
	var args struct {
		Payload   string          `json:"payload"`
		UserID    json.RawMessage `json:"user_id"`
		GroupID   json.RawMessage `json:"group_id"`
		UserKey   string          `json:"user_key"`
		GroupKey  string          `json:"group_key"`
		BackendID string          `json:"backend_id"`
		RequestID string          `json:"request_id"`
		Audience  string          `json:"audience"`
		GroupRole json.RawMessage `json:"group_role"`
	}
	if err := req.BindArguments(&args); err != nil {
		return mcp.NewToolResultError("invalid call_ws arguments"), nil
	}
	if args.Audience == "" {
		args.Audience = "group"
	}
	groupRole := ""
	if len(args.GroupRole) != 0 {
		if args.Audience == "private" {
			return bridgeToolResult(bridge.Result{BackendID: args.BackendID, RequestID: args.RequestID, Audience: args.Audience, Status: "failed", Outputs: []bridge.Output{}}, bridge.ErrInvalidRequest), nil
		}
		roleJSON := bytes.TrimSpace(args.GroupRole)
		if len(roleJSON) == 0 || roleJSON[0] != '"' || json.Unmarshal(roleJSON, &groupRole) != nil || (groupRole != "" && groupRole != "owner" && groupRole != "admin" && groupRole != "member") {
			return bridgeToolResult(bridge.Result{BackendID: args.BackendID, RequestID: args.RequestID, Audience: args.Audience, Status: "failed", Outputs: []bridge.Output{}}, bridge.ErrInvalidRequest), nil
		}
	}
	if args.BackendID == "" || args.RequestID == "" || args.Payload == "" {
		return bridgeToolResult(bridge.Result{BackendID: args.BackendID, RequestID: args.RequestID, Audience: args.Audience, Status: "failed", Outputs: []bridge.Output{}}, nil), nil
	}
	if (args.UserKey != "" && len(args.UserID) != 0) || (args.GroupKey != "" && len(args.GroupID) != 0) {
		return bridgeToolResult(bridge.Result{BackendID: args.BackendID, RequestID: args.RequestID, Audience: args.Audience, Status: "failed", Outputs: []bridge.Output{}}, nil), nil
	}
	var userID, groupID int64
	var err error
	if args.UserKey != "" {
		userID, err = wsBridge.MapIdentity(args.BackendID, "user", args.UserKey)
	} else {
		userID, err = parseCompatOneBotID(args.UserID)
	}
	if err != nil {
		return bridgeToolResult(bridge.Result{BackendID: args.BackendID, RequestID: args.RequestID, Audience: args.Audience, Status: "failed", Outputs: []bridge.Output{}}, nil), nil
	}
	if args.Audience == "group" {
		if args.GroupKey != "" {
			groupID, err = wsBridge.MapIdentity(args.BackendID, "group", args.GroupKey)
		} else {
			groupID, err = parseCompatOneBotID(args.GroupID)
		}
	} else if len(args.GroupID) != 0 || args.GroupKey != "" {
		err = bridge.ErrInvalidRequest
	}
	if err != nil {
		return bridgeToolResult(bridge.Result{BackendID: args.BackendID, RequestID: args.RequestID, Audience: args.Audience, Status: "failed", Outputs: []bridge.Output{}}, nil), nil
	}
	bridgeRequest := bridge.Request{BackendID: args.BackendID, RequestID: args.RequestID, Audience: args.Audience, Payload: args.Payload, UserKey: args.UserKey, GroupKey: args.GroupKey, UserID: userID, GroupID: groupID, GroupRole: groupRole}
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	unregister, registered := registerRPCCancel(callCtx, cancel)
	if !registered {
		return mcp.NewToolResultError("duplicate active MCP request id"), nil
	}
	defer unregister()
	result, err := wsBridge.Call(callCtx, bridgeRequest, func(dispatchCtx context.Context, connectionID, socketID string, sourceID int32) error {
		wsClientsMu.RLock()
		client := wsBackendClients[args.BackendID]
		wsClientsMu.RUnlock()
		if client == nil {
			return bridge.ErrBackendUnavailable
		}
		event, buildErr := Processor.BuildBridgeEventWithRole(args.Payload, args.Audience, userID, groupID, sourceID, groupRole)
		if buildErr != nil {
			return bridge.ErrInvalidRequest
		}
		_ = connectionID // the event is emitted only through the captured socket ID.
		return client.SendBridgeMessage(dispatchCtx, socketID, event)
	})
	if result.Outputs == nil {
		status := "failed"
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			status = "unknown"
		}
		result = bridge.Result{BackendID: args.BackendID, RequestID: args.RequestID, Audience: args.Audience, Status: status, Outputs: []bridge.Output{}}
	}
	if errors.Is(err, bridge.ErrRequestConflict) {
		return mcp.NewToolResultError("request_id conflicts with a prior request"), nil
	}
	return bridgeToolResult(result, err), nil
}

func bridgeToolResult(result bridge.Result, _ error) *mcp.CallToolResult {
	if result.Outputs == nil {
		result.Outputs = []bridge.Output{}
	}
	data, err := json.Marshal(result)
	if err != nil {
		return mcp.NewToolResultText(`{"status":"unknown","outputs":[]}`)
	}
	return mcp.NewToolResultText(string(data))
}

func parseCompatOneBotID(raw json.RawMessage) (int64, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || string(raw) == "null" {
		return 0, bridge.ErrInvalidRequest
	}
	var value string
	if raw[0] == '"' {
		if err := json.Unmarshal(raw, &value); err != nil {
			return 0, bridge.ErrInvalidRequest
		}
	} else {
		value = string(raw)
	}
	if value == "" || (len(value) > 1 && value[0] == '0') {
		return 0, bridge.ErrInvalidRequest
	}
	for _, char := range value {
		if char < '0' || char > '9' {
			return 0, bridge.ErrInvalidRequest
		}
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed <= 0 || parsed > maxSafeOneBotID || strconv.FormatInt(parsed, 10) != value {
		return 0, bridge.ErrInvalidRequest
	}
	return parsed, nil
}

func compatOneBotIDSchema(description string) map[string]any {
	return map[string]any{
		"description": description + ": safe positive integer or canonical decimal string",
		"oneOf": []any{
			map[string]any{"type": "integer", "minimum": int64(1), "maximum": maxSafeOneBotID},
			map[string]any{"type": "string", "pattern": "^[1-9][0-9]{0,15}$", "maxLength": 16},
		},
	}
}

// The bridge remains optional. Keep the original request and reply behavior
// byte-for-byte in this branch for existing non-bridge deployments.
func callWSLegacy(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var args struct {
		Payload string `json:"payload"`
		UserID  string `json:"user_id"`
		GroupID string `json:"group_id"`
		Timeout int    `json:"timeout"`
	}
	if err := req.BindArguments(&args); err != nil {
		return mcp.NewToolResultErrorFromErr("参数解析失败", err), err
	}
	if args.Payload == "" {
		args.Payload = "帮助"
	}
	if err := Processor.ProcessGroupMessage(req, wsClients); err != nil {
		return mcp.NewToolResultErrorFromErr("消息派发失败", err), nil
	}
	timeout := config.GetTimeOut()
	message, err := wsclient.WaitForActionMessage(args.UserID, time.Duration(timeout)*time.Second)
	if err != nil {
		return mcp.NewToolResultText("等待超时"), nil
	}
	var text string
	if messageText, ok := message.Params.Message.(string); ok {
		text = messageText
	} else {
		text = praser.ParseMessageContent(message.Params.Message, false)
	}
	messageType, resultText, resultImg, err := ProcessMessage(text, message)
	if err != nil {
		return mcp.NewToolResultText("处理错误"), nil
	}
	switch messageType {
	case 1:
		if result, ok := resultText.(string); ok {
			pending, _, pendingErr := wsclient.GetPendingMessages(args.UserID, true, len(result))
			if pendingErr == nil {
				for _, prior := range pending {
					var content string
					if raw, ok := prior.Params.Message.(string); ok {
						content = raw
					} else {
						content = praser.ParseMessageContent(prior.Params.Message, true)
					}
					result = fmt.Sprintf("%s\n-----历史信息----\n%s", content, result)
				}
			}
			return mcp.NewToolResultText(result), nil
		}
	case 2, 4:
		imageData, imageErr := ImageURLToBase64(resultImg.(string))
		if imageErr != nil {
			return nil, imageErr
		}
		return mcp.NewToolResultImage(resultText.(string), imageData, "image/jpeg"), nil
	}
	return mcp.NewToolResultText("未知类型信息"), nil
}

// ProcessMessage 处理信息并归类
func ProcessMessage(input string, rawMsg *callapi.ActionMessage) (int, interface{}, interface{}, error) {
	// 正则表达式定义
	httpUrlImagePattern := regexp.MustCompile(`\[CQ:image,file=http://(.+?)\]`)
	httpsUrlImagePattern := regexp.MustCompile(`\[CQ:image,file=https://(.+?)\]`)
	base64ImagePattern := regexp.MustCompile(`\[CQ:image,file=base64://(.+?)\]`)
	base64RecordPattern := regexp.MustCompile(`\[CQ:record,file=base64://(.+?)\]`)
	httpUrlRecordPattern := regexp.MustCompile(`\[CQ:record,file=http://(.+?)\]`)
	httpsUrlRecordPattern := regexp.MustCompile(`\[CQ:record,file=https://(.+?)\]`)

	// 检查是否含有base64编码的图片或语音信息
	var err error
	if base64ImagePattern.MatchString(input) || base64RecordPattern.MatchString(input) {
		input, err = processInput(input)
		if err != nil {
			log.Printf("processInput出错:\n%v\n", err)
		}
		log.Printf("处理后的base64编码的图片或语音信息:\n%v\n", input)
	}

	// 检查是否为纯文本信息
	if !httpUrlImagePattern.MatchString(input) && !httpsUrlImagePattern.MatchString(input) && !httpUrlRecordPattern.MatchString(input) && !httpsUrlRecordPattern.MatchString(input) {
		// 使用正则表达式匹配并替换[CQ:at,qq=x]格式的信息
		cqAtPattern := regexp.MustCompile(`\[CQ:at,qq=\d+\]`)
		// 将匹配到的部分替换为空字符串
		filteredInput := cqAtPattern.ReplaceAllString(input, "")

		// 返回过滤后的纯文本信息
		return 1, filteredInput, nil, nil
	}

	// 图片信息处理
	if httpUrlImagePattern.MatchString(input) || httpsUrlImagePattern.MatchString(input) {
		// 合并匹配到的所有图片URL
		httpImageUrls := httpUrlImagePattern.FindAllStringSubmatch(input, -1)
		httpsImageUrls := httpsUrlImagePattern.FindAllStringSubmatch(input, -1)

		// 通过前缀重新构造完整的图片URL
		var imageUrls []string
		for _, match := range httpImageUrls {
			imageUrls = append(imageUrls, "http://"+match[1])
		}
		for _, match := range httpsImageUrls {
			imageUrls = append(imageUrls, "https://"+match[1])
		}

		// 替换掉所有图片标签
		input = httpUrlImagePattern.ReplaceAllString(input, "")
		input = httpsUrlImagePattern.ReplaceAllString(input, "")

		// 如果替换后内容为空 且只有一个图片
		if len(imageUrls) == 1 && input == "" {
			imgUrl := imageUrls[0]
			return 2, "", imgUrl, nil // 纯图片信息
		} else {
			// 图片信息+文本
			imgUrl := imageUrls[0]

			// 将文字部分加入到堆积的事件中
			//wsclient.AddMessageToPending(rawMsg.Params.UserID.(string), rawMsg)
			return 2, input, imgUrl, nil // 图文信息
		}
	}

	// 语音信息处理
	if httpUrlRecordPattern.MatchString(input) || httpsUrlRecordPattern.MatchString(input) {
		// 初始化变量用于存放处理后的URL
		var recordUrl string

		// 查找匹配的HTTP URL
		httpRecordMatches := httpUrlRecordPattern.FindAllStringSubmatch(input, -1)
		if len(httpRecordMatches) > 0 {
			// 取第一个匹配项，并添加HTTP前缀
			recordUrl = "http://" + httpRecordMatches[0][1]
		}

		// 查找匹配的HTTPS URL
		httpsRecordMatches := httpsUrlRecordPattern.FindAllStringSubmatch(input, -1)
		if len(httpsRecordMatches) > 0 {
			// 如果已经找到HTTP URL，优先处理HTTPS URL
			recordUrl = "https://" + httpsRecordMatches[0][1]
		}

		// 如果找到了语音URL
		if recordUrl != "" {
			mediaId := 0
			return 3, mediaId, nil, nil // 纯语音信息
		}
	}

	// 如果没有匹配到任何已知格式，返回错误
	return 0, nil, nil, errors.New("unknown message format")
}

// processInput 处理含有Base64编码的图片和语音信息的字符串
func processInput(input string) (string, error) {

	return input, nil
}

// 转为json并打印
func PrintCallToolRequestAsJSON(req mcp.CallToolRequest) error {
	data, err := json.MarshalIndent(req, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}

// NewToolResultTwoTexts creates a new CallToolResult with two text content elements
func NewToolResultTwoTexts(text1, text2 string) *mcp.CallToolResult {
	return &mcp.CallToolResult{
		Content: []mcp.Content{
			mcp.TextContent{
				Type: "text",
				Text: text1,
			},
			mcp.TextContent{
				Type: "text",
				Text: text2,
			},
		},
	}
}

// ImageURLToBase64 downloads an image from a URL and returns its base64 encoding as a string
func ImageURLToBase64(url string) (string, error) {
	// 1. 发起 GET 请求
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	// 2. 读取所有内容到内存
	imgData, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	// 3. base64 编码
	base64Str := base64.StdEncoding.EncodeToString(imgData)
	return base64Str, nil
}
