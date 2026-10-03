package wsclient

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hoshinonyaruko/gensokyo-mcp/botstats"
	"github.com/hoshinonyaruko/gensokyo-mcp/bridge"
	"github.com/hoshinonyaruko/gensokyo-mcp/callapi"
	"github.com/hoshinonyaruko/gensokyo-mcp/config"
	"github.com/hoshinonyaruko/gensokyo-mcp/mylog"
	bolt "go.etcd.io/bbolt"
)

var activeBridge *bridge.Manager
var bridgeManagerMu sync.RWMutex
var legacyMapMu sync.Mutex
var legacyPendingMu sync.Mutex
var legacyEchoToChannel = map[string]chan callapi.ActionMessage{}
var legacyPendingMessages = map[string][]callapi.ActionMessage{}
var legacyIDMu sync.Mutex
var legacyIDDB *bolt.DB

// OpenMessageIDAllocator enables the persisted int32 allocator in ordinary
// mode, where no bridge Manager owns the shared protocol allocator.
func OpenMessageIDAllocator(path string) error {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("protocol_ids"))
		if err != nil {
			return err
		}
		if bucket.Get([]byte("next_message_id")) == nil {
			var lastAllocated [4]byte
			return bucket.Put([]byte("next_message_id"), lastAllocated[:])
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		return err
	}
	legacyIDMu.Lock()
	old := legacyIDDB
	legacyIDDB = db
	legacyIDMu.Unlock()
	if old != nil {
		_ = old.Close()
	}
	return nil
}

func CloseMessageIDAllocator() error {
	legacyIDMu.Lock()
	db := legacyIDDB
	legacyIDDB = nil
	legacyIDMu.Unlock()
	if db != nil {
		return db.Close()
	}
	return nil
}

func nextLegacyMessageID() (int32, error) {
	legacyIDMu.Lock()
	defer legacyIDMu.Unlock()
	if legacyIDDB == nil {
		return 0, fmt.Errorf("message ID allocator is unavailable")
	}
	var next int32
	err := legacyIDDB.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("protocol_ids"))
		raw := bucket.Get([]byte("next_message_id"))
		if len(raw) != 4 {
			return fmt.Errorf("message ID allocator state is invalid")
		}
		value := binary.BigEndian.Uint32(raw)
		if value >= uint32(^uint32(0)>>1) {
			return fmt.Errorf("message ID space exhausted")
		}
		next = int32(value + 1)
		var encoded [4]byte
		binary.BigEndian.PutUint32(encoded[:], uint32(next))
		return bucket.Put([]byte("next_message_id"), encoded[:])
	})
	return next, err
}

// AllocateProtocolMessageID returns the next persisted positive int32 ID used
// by ordinary OneBot events and standard action acknowledgements.
func AllocateProtocolMessageID() (int32, error) {
	return nextLegacyMessageID()
}

func SetBridgeManager(manager *bridge.Manager) {
	bridgeManagerMu.Lock()
	activeBridge = manager
	bridgeManagerMu.Unlock()
}

type WebSocketClient struct {
	conn           *websocket.Conn
	connMu         sync.RWMutex
	botID          uint64
	backendID      string
	urlStr         string
	cancel         context.CancelFunc
	isReconnecting atomic.Bool
	socketSequence atomic.Uint64
	writeCh        chan writeRequest // 写请求通道
	closeCh        chan struct{}     // 用于关闭的通道
	closeOnce      sync.Once
	runCtx         context.Context
	runCancel      context.CancelFunc
	loopWG         sync.WaitGroup
	sessionMu      sync.RWMutex
	sessions       map[string]*websocket.Conn
	registrations  map[string]string
	socketIDs      map[*websocket.Conn]string
}

type writeRequest struct {
	messageType int
	data        []byte
	result      chan error
	conn        *websocket.Conn
	ctx         context.Context
}

// SendMessage 发送消息，将写请求发送到写 Goroutine
func (client *WebSocketClient) SendMessage(message map[string]interface{}) error {
	return client.sendMessage(message)
}

// SendOneShotMessage writes a bridge event once and never queues it for replay.
func (client *WebSocketClient) SendOneShotMessage(message map[string]interface{}) error {
	return client.sendMessage(message)
}

func (client *WebSocketClient) sendMessage(message map[string]interface{}) error {
	client.connMu.RLock()
	conn := client.conn
	client.connMu.RUnlock()
	return client.sendMessageOnContext(context.Background(), conn, message)
}

func (client *WebSocketClient) SendBridgeMessage(ctx context.Context, socketID string, message map[string]interface{}) error {
	client.sessionMu.RLock()
	conn := client.sessions[socketID]
	client.sessionMu.RUnlock()
	if conn == nil {
		return fmt.Errorf("websocket registration is no longer active")
	}
	return client.sendMessageOnContext(ctx, conn, message)
}

func (client *WebSocketClient) sendMessageOnContext(ctx context.Context, conn *websocket.Conn, message map[string]interface{}) error {
	if conn == nil {
		return fmt.Errorf("websocket is not connected")
	}
	// 序列化消息
	msgBytes, err := json.Marshal(message)
	if err != nil {
		log.Println("Error marshalling message:", err)
		return err
	}

	// 创建专用通道，用于接收写操作的结果
	result := make(chan error, 1)
	req := writeRequest{
		messageType: websocket.TextMessage,
		data:        msgBytes,
		result:      result,
		conn:        conn,
		ctx:         ctx,
	}
	select {
	case <-client.closeCh:
		return fmt.Errorf("websocket client is closed")
	case <-ctx.Done():
		return ctx.Err()
	case client.writeCh <- req:
	}
	select {
	case <-client.closeCh:
		return fmt.Errorf("websocket client is closed")
	case <-ctx.Done():
		return ctx.Err()
	case err := <-result:
		return err
	}
}

// Close 关闭 WebSocketClient，停止写 Goroutine
func (client *WebSocketClient) Close() error {
	client.closeOnce.Do(func() {
		close(client.closeCh)
		if client.runCancel != nil {
			client.runCancel()
		}
		client.connMu.Lock()
		if client.cancel != nil {
			client.cancel()
		}
		conn := client.conn
		client.connMu.Unlock()
		if conn != nil {
			_ = conn.Close()
		}
	})
	client.loopWG.Wait()
	return nil
}

// startWriter 专用的写 Goroutine
func (client *WebSocketClient) startWriter() {
	for {
		select {
		case req := <-client.writeCh:
			var err error
			if req.ctx != nil && req.ctx.Err() != nil {
				err = req.ctx.Err()
			} else if req.conn == nil {
				err = fmt.Errorf("websocket is not connected")
			} else {
				deadline := time.Now().Add(5 * time.Second)
				if req.ctx != nil {
					if ctxDeadline, ok := req.ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
						deadline = ctxDeadline
					}
				}
				if err = req.conn.SetWriteDeadline(deadline); err == nil && (req.ctx == nil || req.ctx.Err() == nil) {
					err = req.conn.WriteMessage(req.messageType, req.data)
				} else if err == nil {
					err = req.ctx.Err()
				}
			}
			if err != nil {
				mylog.Printf("OneBot websocket write failed: %s", safeWriteError(err))
			}
			if req.result != nil {
				req.result <- err
			}
		case <-client.closeCh:
			return
		}
	}
}

// 处理onebotv11应用端发来的信息
func (client *WebSocketClient) handleIncomingMessages(cancel context.CancelFunc) {
	client.connMu.RLock()
	conn := client.conn
	client.connMu.RUnlock()
	client.handleIncomingMessagesOn(conn, cancel)
}

func (client *WebSocketClient) handleIncomingMessagesOn(conn *websocket.Conn, cancel context.CancelFunc) {
	socketID := client.attachSocket(conn)
	if getBridgeManager() != nil && conn != nil {
		conn.SetReadLimit(1024 * 1024)
	}
	for {
		if conn == nil {
			return
		}
		_, msg, err := conn.ReadMessage()
		if err != nil {
			mylog.Printf("OneBot websocket disconnected from %s", safeEndpoint(client.urlStr))
			cancel() // 取消心跳 goroutine
			bridgeManagerMu.RLock()
			manager := activeBridge
			bridgeManagerMu.RUnlock()
			if manager != nil && client.backendID != "" {
				manager.Disconnect(client.backendID, socketID)
			}
			client.detachSocket(socketID, conn)
			if getBridgeManager() == nil {
				go client.Reconnect()
			}
			return // 退出循环，不再尝试读取消息
		}

		client.recvMessageOn(conn, msg)
	}
}

func (client *WebSocketClient) attachSocket(conn *websocket.Conn) string {
	client.sessionMu.Lock()
	defer client.sessionMu.Unlock()
	if id := client.socketIDs[conn]; id != "" {
		return id
	}
	if client.sessions == nil {
		client.sessions = make(map[string]*websocket.Conn)
	}
	if client.registrations == nil {
		client.registrations = make(map[string]string)
	}
	if client.socketIDs == nil {
		client.socketIDs = make(map[*websocket.Conn]string)
	}
	socketID := fmt.Sprintf("socket-%d", client.socketSequence.Add(1))
	client.socketIDs[conn] = socketID
	client.sessions[socketID] = conn
	return socketID
}

func (client *WebSocketClient) detachSocket(socketID string, conn *websocket.Conn) {
	client.sessionMu.Lock()
	if client.sessions[socketID] == conn {
		delete(client.sessions, socketID)
		delete(client.socketIDs, conn)
	}
	delete(client.registrations, socketID)
	client.sessionMu.Unlock()
}

func (client *WebSocketClient) registrationFor(conn *websocket.Conn) (string, bool) {
	client.sessionMu.RLock()
	defer client.sessionMu.RUnlock()
	socketID := client.socketIDs[conn]
	registered, ok := client.registrations[socketID]
	return registered, ok && socketID != "" && client.sessions[socketID] == conn
}

func (client *WebSocketClient) setRegistration(conn *websocket.Conn, connectionID string) {
	client.sessionMu.Lock()
	if socketID := client.socketIDs[conn]; socketID != "" && client.sessions[socketID] == conn {
		client.registrations[socketID] = connectionID
	}
	client.sessionMu.Unlock()
}

func safeWriteError(err error) string {
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline exceeded"
	}
	return "write failed"
}

// 断线重连
func (client *WebSocketClient) Reconnect() {
	if !client.isReconnecting.CompareAndSwap(false, true) {
		return
	}
	defer client.isReconnecting.Store(false)

	addresses := config.GetWsAddress()
	tokens := config.GetWsToken()

	var token string
	for index, address := range addresses {
		if address == client.urlStr && index < len(tokens) {
			token = tokens[index]
			break
		}
	}

	// 检查URL中是否有access_token参数
	mp := getParamsFromURI(client.urlStr)
	if val, ok := mp["access_token"]; ok {
		token = val
	}

	headers := http.Header{
		"User-Agent":    []string{"CQHttp/4.15.0"},
		"X-Client-Role": []string{"Universal"},
		"X-Self-ID":     []string{fmt.Sprintf("%d", client.botID)},
	}

	if token != "" {
		scheme := "Token"
		if getBridgeManager() != nil {
			scheme = "Bearer"
		}
		headers["Authorization"] = []string{scheme + " " + token}
	}
	mylog.Printf("reconnecting to WebSocket endpoint %s", safeEndpoint(client.urlStr))
	dialer := websocket.Dialer{
		Proxy:            http.ProxyFromEnvironment,
		HandshakeTimeout: 45 * time.Second,
	}

	var conn *websocket.Conn
	var err error

	maxRetryAttempts := config.GetReconnecTimes()
	retryCount := 0
	for {
		mylog.Println("Dialing WebSocket endpoint:", safeEndpoint(client.urlStr))
		conn, _, err = dialer.Dial(client.urlStr, headers)
		if err != nil {
			retryCount++
			if retryCount > maxRetryAttempts {
				mylog.Printf("Exceeded maximum retry attempts for WebSocket[%s]", safeEndpoint(client.urlStr))
				return
			}
			mylog.Printf("Failed to connect to WebSocket[%s], retrying in 5 seconds...\n", safeEndpoint(client.urlStr))
			time.Sleep(5 * time.Second) // sleep for 5 seconds before retrying
		} else {
			mylog.Printf("Successfully connected to %s.\n", safeEndpoint(client.urlStr))
			break // successfully connected, break the loop
		}
	}
	// 复用现有的client完成重连
	client.connMu.Lock()
	oldConn := client.conn
	client.conn = conn
	client.connMu.Unlock()
	if oldConn != nil {
		_ = oldConn.Close()
	}

	// 再次发送元事件
	message := map[string]interface{}{
		"meta_event_type": "lifecycle",
		"post_type":       "meta_event",
		"self_id":         client.botID,
		"sub_type":        "connect",
		"time":            int(time.Now().Unix()),
	}

	mylog.Printf("Message: %+v\n", message)

	err = client.SendMessage(message)
	if err != nil {
		// handle error
		mylog.Printf("Error sending message: %v\n", err)
	}

	//退出老的sendHeartbeat和handleIncomingMessages
	client.cancel()

	// Starting goroutine for heartbeats and another for listening to messages
	ctx, cancel := context.WithCancel(context.Background())

	client.cancel = cancel
	heartbeatinterval := config.GetHeartBeatInterval()
	go client.sendHeartbeat(ctx, client.botID, heartbeatinterval)
	go client.handleIncomingMessages(cancel)

	mylog.Printf("Successfully reconnected to WebSocket.")

}

// 处理发送失败的消息
// 处理信息,调用腾讯api
type actionEnvelope struct {
	Action string          `json:"action"`
	Params json.RawMessage `json:"params"`
	Echo   json.RawMessage `json:"echo"`
}

func (client *WebSocketClient) recvMessage(msg []byte) {
	client.connMu.RLock()
	conn := client.conn
	client.connMu.RUnlock()
	client.recvMessageOn(conn, msg)
}

func (client *WebSocketClient) recvMessageOn(conn *websocket.Conn, msg []byte) {
	var wire actionEnvelope
	if err := json.Unmarshal(msg, &wire); err != nil || wire.Action == "" {
		mylog.Println("Rejected malformed OneBot action")
		return
	}
	var params callapi.ParamsContent
	if len(wire.Params) != 0 && string(wire.Params) != "null" {
		if err := json.Unmarshal(wire.Params, &params); err != nil {
			if manager := getBridgeManager(); manager != nil {
				_ = client.writeBridgeAck(conn, wire.Echo, false, nil, "invalid action parameters")
			} else {
				client.writeLegacyAck(conn, wire.Echo, false, nil, "invalid action parameters", 1002)
			}
			return
		}
	}
	manager := getBridgeManager()
	if manager == nil {
		client.recvLegacyAction(conn, wire, params)
		return
	}
	socketID := client.attachSocket(conn)
	switch wire.Action {
	case "_llm_bridge_register":
		client.handleBridgeRegister(conn, socketID, manager, wire)
		return
	case "_llm_bridge_complete":
		client.handleBridgeComplete(conn, manager, wire)
		return
	case "send_group_msg", "send_private_msg", "send_msg":
		client.handleBridgeSend(conn, manager, wire)
		return
	default:
		if strings.HasPrefix(wire.Action, "send") {
			_ = client.writeBridgeAck(conn, wire.Echo, false, nil, "unsupported send action")
			return
		}
		client.respondToActionOn(conn, wire.Action, echoValue(wire.Echo), params)
	}
}

func (client *WebSocketClient) recvLegacyAction(conn *websocket.Conn, wire actionEnvelope, params callapi.ParamsContent) {
	if !strings.HasPrefix(wire.Action, "send") {
		client.respondToActionOn(conn, wire.Action, echoValue(wire.Echo), params)
		return
	}
	if wire.Action != "send_group_msg" && wire.Action != "send_private_msg" && wire.Action != "send_msg" {
		client.writeLegacyAck(conn, wire.Echo, false, nil, "unsupported action", 1404)
		return
	}
	if !validLegacySendTarget(wire.Action, params) {
		client.writeLegacyAck(conn, wire.Echo, false, nil, "invalid message target", 1001)
		return
	}
	messageID, err := nextLegacyMessageID()
	if err != nil {
		client.writeLegacyAck(conn, wire.Echo, false, nil, "message ID unavailable", 1002)
		return
	}
	message := callapi.ActionMessage{Action: wire.Action, Params: params}
	if len(wire.Echo) != 0 {
		_ = json.Unmarshal(wire.Echo, &message.Echo)
	}
	key := fmt.Sprint(params.UserID)
	legacyMapMu.Lock()
	channel := legacyEchoToChannel[key]
	if channel != nil {
		delete(legacyEchoToChannel, key)
	}
	legacyMapMu.Unlock()
	if channel != nil {
		select {
		case channel <- message:
		default:
		}
	} else {
		legacyPendingMu.Lock()
		legacyPendingMessages[key] = append(legacyPendingMessages[key], message)
		legacyPendingMu.Unlock()
	}
	client.writeLegacyAck(conn, wire.Echo, true, map[string]interface{}{"message_id": messageID}, "", 0)
}

func validLegacySendTarget(action string, params callapi.ParamsContent) bool {
	hasGroup := params.GroupID != nil && strings.TrimSpace(fmt.Sprint(params.GroupID)) != ""
	hasUser := params.UserID != nil && strings.TrimSpace(fmt.Sprint(params.UserID)) != ""
	switch action {
	case "send_group_msg":
		return hasGroup && !hasUser
	case "send_private_msg":
		return hasUser && !hasGroup
	case "send_msg":
		switch params.MessageType {
		case "group":
			return hasGroup && !hasUser
		case "private":
			return hasUser && !hasGroup
		case "":
			return hasGroup != hasUser
		default:
			return false
		}
	default:
		return false
	}
}

func (client *WebSocketClient) writeLegacyAck(conn *websocket.Conn, echo json.RawMessage, ok bool, data interface{}, reason string, retcode int) {
	status := "failed"
	if ok {
		status, retcode, reason = "ok", 0, ""
	}
	response := map[string]interface{}{"status": status, "retcode": retcode, "data": data, "message": reason}
	if len(echo) > 0 {
		response["echo"] = json.RawMessage(echo)
	}
	if err := client.sendMessageOnContext(context.Background(), conn, response); err != nil {
		mylog.Println("OneBot action acknowledgement failed")
	}
}

// WaitForActionMessage retains the pre-bridge command flow when bridge mode is off.
func WaitForActionMessage(userID string, timeout time.Duration) (*callapi.ActionMessage, error) {
	channel := make(chan callapi.ActionMessage, 1)
	legacyMapMu.Lock()
	legacyEchoToChannel[userID] = channel
	legacyMapMu.Unlock()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case message := <-channel:
		return &message, nil
	case <-timer.C:
		legacyMapMu.Lock()
		if legacyEchoToChannel[userID] == channel {
			delete(legacyEchoToChannel, userID)
		}
		legacyMapMu.Unlock()
		return nil, fmt.Errorf("timeout waiting for OneBot action reply")
	}
}

func GetPendingMessages(userID string, clear bool, currentLength int) ([]callapi.ActionMessage, int, error) {
	legacyPendingMu.Lock()
	defer legacyPendingMu.Unlock()
	messages := legacyPendingMessages[userID]
	var selected []callapi.ActionMessage
	total := currentLength
	consumed := 0
	for _, message := range messages {
		content := ""
		if text, ok := message.Params.Message.(string); ok {
			content = text
		} else {
			content = fmt.Sprint(message.Params.Message)
		}
		if total+len(content)+len("-----历史信息----") > 2047 {
			break
		}
		selected = append(selected, message)
		total += len(content) + len("-----历史信息----")
		consumed++
	}
	if clear {
		legacyPendingMessages[userID] = append([]callapi.ActionMessage(nil), messages[consumed:]...)
	}
	return selected, total, nil
}

func getBridgeManager() *bridge.Manager {
	bridgeManagerMu.RLock()
	defer bridgeManagerMu.RUnlock()
	return activeBridge
}

func echoValue(echo json.RawMessage) interface{} {
	if len(echo) == 0 {
		return nil
	}
	return json.RawMessage(echo)
}

func (client *WebSocketClient) handleBridgeRegister(conn *websocket.Conn, socketID string, manager *bridge.Manager, wire actionEnvelope) {
	var params bridge.RegisterParams
	if err := decodeStrict(wire.Params, &params); err != nil {
		_ = client.writeBridgeAck(conn, wire.Echo, false, nil, "registration rejected")
		return
	}
	connectionID, err := manager.Register(client.backendID, socketID, params)
	if err != nil {
		_ = client.writeBridgeAck(conn, wire.Echo, false, nil, "registration rejected")
		return
	}
	data := map[string]interface{}{"version": 1, "connection_id": connectionID}
	if err := client.writeBridgeAck(conn, wire.Echo, true, data, ""); err != nil {
		manager.Disconnect(client.backendID, socketID)
		return
	}
	client.setRegistration(conn, connectionID)
	if err := manager.Activate(client.backendID, socketID, connectionID); err != nil {
		manager.Disconnect(client.backendID, socketID)
		client.sessionMu.Lock()
		delete(client.registrations, socketID)
		client.sessionMu.Unlock()
	}
}

func (client *WebSocketClient) handleBridgeComplete(conn *websocket.Conn, manager *bridge.Manager, wire actionEnvelope) {
	var params struct {
		Version         int             `json:"version"`
		SourceMessageID json.RawMessage `json:"source_message_id"`
		ConnectionID    string          `json:"connection_id"`
		Status          string          `json:"status"`
		OutputCount     int             `json:"output_count"`
	}
	if err := decodeStrict(wire.Params, &params); err != nil || params.Version != 1 || params.OutputCount < 0 {
		_ = client.writeBridgeAck(conn, wire.Echo, false, nil, "completion rejected")
		return
	}
	registeredConnection, registered := client.registrationFor(conn)
	if !registered || registeredConnection != params.ConnectionID {
		_ = client.writeBridgeAck(conn, wire.Echo, false, nil, "completion rejected")
		return
	}
	sourceID, err := strictMessageID(params.SourceMessageID)
	if err == nil {
		err = manager.Complete(client.backendID, params.ConnectionID, sourceID, params.Status, params.OutputCount)
	}
	_ = client.writeBridgeAck(conn, wire.Echo, err == nil, nil, ackMessage(err, "completion rejected"))
}

func (client *WebSocketClient) handleBridgeSend(conn *websocket.Conn, manager *bridge.Manager, wire actionEnvelope) {
	var params struct {
		GroupID     json.RawMessage `json:"group_id"`
		UserID      json.RawMessage `json:"user_id"`
		MessageType string          `json:"message_type"`
		AutoEscape  bool            `json:"auto_escape"`
		Message     json.RawMessage `json:"message"`
	}
	if err := decodeStrict(wire.Params, &params); err != nil {
		_ = client.writeBridgeAck(conn, wire.Echo, false, nil, "invalid send parameters")
		return
	}
	connectionID, registered := client.registrationFor(conn)
	if !registered {
		_ = client.writeBridgeAck(conn, wire.Echo, false, nil, "unregistered connection")
		return
	}
	audience, targetID, ok := bridgeOutputTarget(wire.Action, params.GroupID, params.UserID, params.MessageType)
	if !ok {
		_ = client.writeBridgeAck(conn, wire.Echo, false, nil, "invalid message target")
		return
	}
	text, sourceMessageID, ok := extractCorrelatedTextWithOptions(params.Message, params.AutoEscape)
	if !ok {
		_ = client.writeBridgeAck(conn, wire.Echo, false, nil, "reply correlation rejected")
		return
	}
	messageID, err := manager.NextMessageID()
	if err != nil {
		_ = client.writeBridgeAck(conn, wire.Echo, false, nil, "message ID unavailable")
		return
	}
	if err := manager.Deliver(client.backendID, connectionID, sourceMessageID, wire.Action, audience, targetID, text); err != nil {
		_ = client.writeBridgeAck(conn, wire.Echo, false, nil, "output does not match the active request")
		return
	}
	_ = client.writeBridgeAck(conn, wire.Echo, true, map[string]interface{}{"message_id": messageID}, "")
}

func (client *WebSocketClient) writeBridgeAck(conn *websocket.Conn, echo json.RawMessage, ok bool, data interface{}, reason string) error {
	status, retcode, message := "failed", 1002, reason
	if ok {
		status, retcode, message = "ok", 0, ""
	}
	response := map[string]interface{}{"status": status, "retcode": retcode, "data": data, "message": message}
	if len(echo) != 0 {
		response["echo"] = json.RawMessage(echo)
	}
	return client.sendMessageOnContext(context.Background(), conn, response)
}

func ackMessage(err error, failure string) string {
	if err == nil {
		return ""
	}
	return failure
}

func decodeStrict(data json.RawMessage, target interface{}) error {
	if len(data) == 0 {
		return fmt.Errorf("missing params")
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra interface{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("multiple values")
		}
		return err
	}
	return nil
}

func strictMessageID(raw json.RawMessage) (int32, error) {
	value, err := bridge.DecodeJSONNumber(raw)
	if err != nil || value > int64(^uint32(0)>>1) {
		return 0, fmt.Errorf("invalid source message ID")
	}
	return int32(value), nil
}

func bridgeOutputTarget(action string, groupRaw, userRaw json.RawMessage, messageType string) (string, int64, bool) {
	hasGroup := len(groupRaw) > 0 && string(groupRaw) != "null"
	hasUser := len(userRaw) > 0 && string(userRaw) != "null"
	var audience string
	switch action {
	case "send_group_msg":
		if !hasGroup || hasUser {
			return "", 0, false
		}
		audience = "group"
	case "send_private_msg":
		if !hasUser || hasGroup {
			return "", 0, false
		}
		audience = "private"
	case "send_msg":
		if messageType == "" {
			if hasGroup == hasUser {
				return "", 0, false
			}
			if hasGroup {
				messageType = "group"
			} else {
				messageType = "private"
			}
		}
		switch messageType {
		case "group":
			if !hasGroup || hasUser {
				return "", 0, false
			}
			audience = "group"
		case "private":
			if !hasUser || hasGroup {
				return "", 0, false
			}
			audience = "private"
		default:
			return "", 0, false
		}
	default:
		return "", 0, false
	}
	raw := userRaw
	if audience == "group" {
		raw = groupRaw
	}
	target, err := bridge.DecodeJSONNumber(raw)
	return audience, target, err == nil
}

func extractCorrelatedText(raw json.RawMessage) (string, int32, bool) {
	return extractCorrelatedTextWithOptions(raw, false)
}

func extractCorrelatedTextWithOptions(raw json.RawMessage, autoEscape bool) (string, int32, bool) {
	var rawText string
	if json.Unmarshal(raw, &rawText) == nil {
		if autoEscape {
			return "", 0, false
		}
		return extractCorrelatedCQText(rawText)
	}
	var segments []struct {
		Type string                     `json:"type"`
		Data map[string]json.RawMessage `json:"data"`
	}
	if len(raw) == 0 || json.Unmarshal(raw, &segments) != nil || len(segments) == 0 {
		return "", 0, false
	}
	var body strings.Builder
	var sourceID int32
	replies := 0
	for _, segment := range segments {
		switch segment.Type {
		case "reply":
			replies++
			id, err := strictReplyMessageID(segment.Data["id"])
			if err != nil {
				return "", 0, false
			}
			sourceID = id
		case "text":
			textRaw, ok := segment.Data["text"]
			if !ok {
				return "", 0, false
			}
			var text string
			if json.Unmarshal(textRaw, &text) != nil {
				return "", 0, false
			}
			body.WriteString(text)
		default:
			return "", 0, false
		}
	}
	return body.String(), sourceID, replies == 1
}

// OneBot v11 defines reply.data.id as a string, while some implementations
// serialize it as a JSON number. Accept only canonical positive int32 IDs in
// this field; recipient IDs and other strict wire fields stay numeric.
func strictReplyMessageID(raw json.RawMessage) (int32, error) {
	var text string
	if json.Unmarshal(raw, &text) == nil {
		if text == "" || (len(text) > 1 && text[0] == '0') {
			return 0, fmt.Errorf("invalid reply source ID")
		}
		for _, r := range text {
			if r < '0' || r > '9' {
				return 0, fmt.Errorf("invalid reply source ID")
			}
		}
		value, err := strconv.ParseInt(text, 10, 32)
		if err != nil || value <= 0 {
			return 0, fmt.Errorf("invalid reply source ID")
		}
		return int32(value), nil
	}
	return strictMessageID(raw)
}

func extractCorrelatedCQText(raw string) (string, int32, bool) {
	var body strings.Builder
	var sourceID int32
	replies := 0
	for len(raw) > 0 {
		marker := strings.Index(raw, "[CQ:")
		if marker < 0 {
			body.WriteString(unescapeCQText(raw))
			break
		}
		body.WriteString(unescapeCQText(raw[:marker]))
		rest := raw[marker+4:]
		end := strings.IndexByte(rest, ']')
		if end < 0 {
			return "", 0, false
		}
		code := rest[:end]
		if !strings.HasPrefix(code, "reply,id=") || strings.ContainsAny(code, "[]&\r\n") {
			return "", 0, false
		}
		idText := strings.TrimPrefix(code, "reply,id=")
		if idText == "" {
			return "", 0, false
		}
		for _, r := range idText {
			if r < '0' || r > '9' {
				return "", 0, false
			}
		}
		value, err := strconv.ParseInt(idText, 10, 32)
		if err != nil || value <= 0 || strconv.FormatInt(value, 10) != idText {
			return "", 0, false
		}
		sourceID = int32(value)
		replies++
		raw = rest[end+1:]
	}
	return body.String(), sourceID, replies == 1
}

func unescapeCQText(value string) string {
	value = strings.ReplaceAll(value, "&#91;", "[")
	value = strings.ReplaceAll(value, "&#93;", "]")
	value = strings.ReplaceAll(value, "&amp;", "&")
	return value
}

// 发送心跳包
func (client *WebSocketClient) sendHeartbeat(ctx context.Context, botID uint64, heartbeatinterval int) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(heartbeatinterval) * time.Second):
			messageReceived, messageSent, lastMessageTime, err := botstats.GetStats()
			if err != nil {
				mylog.Printf("心跳错误,获取机器人发信状态错误:%v", err)
			}
			message := map[string]interface{}{
				"post_type":       "meta_event",
				"meta_event_type": "heartbeat",
				"time":            int(time.Now().Unix()),
				"self_id":         botID,
				"status": map[string]interface{}{
					"app_enabled":     true,
					"app_good":        true,
					"app_initialized": true,
					"good":            true,
					"online":          true,
					"plugins_good":    nil,
					"stat": map[string]int{
						"packet_received":   34933,
						"packet_sent":       8513,
						"packet_lost":       0,
						"message_received":  messageReceived,
						"message_sent":      messageSent,
						"disconnect_times":  0,
						"lost_times":        0,
						"last_message_time": int(lastMessageTime),
					},
				},
				"interval": 5000, // 以毫秒为单位
			}
			client.SendMessage(message)
		}
	}
}

// NewWebSocketClient 创建 WebSocketClient 实例，接受 WebSocket URL、botID
func NewWebSocketClient(urlStr string, botID uint64, maxRetryAttempts int, backendIDs ...string) (*WebSocketClient, error) {
	addresses := config.GetWsAddress()
	tokens := config.GetWsToken()

	var token string
	for index, address := range addresses {
		if address == urlStr && index < len(tokens) {
			token = tokens[index]
			break
		}
	}

	// 检查URL中是否有access_token参数
	mp := getParamsFromURI(urlStr)
	if val, ok := mp["access_token"]; ok {
		token = val
	}

	backendID := "default"
	if len(backendIDs) > 0 && backendIDs[0] != "" {
		backendID = backendIDs[0]
	}
	mylog.Printf("connecting backend %s to WebSocket endpoint %s", backendID, safeEndpoint(urlStr))
	if getBridgeManager() != nil {
		ctx, cancel := context.WithCancel(context.Background())
		client := &WebSocketClient{
			botID: botID, backendID: backendID, urlStr: urlStr,
			writeCh: make(chan writeRequest, 5000), closeCh: make(chan struct{}),
			runCtx: ctx, runCancel: cancel,
		}
		client.loopWG.Add(2)
		go func() { defer client.loopWG.Done(); client.startWriter() }()
		go func() { defer client.loopWG.Done(); client.bridgeConnectLoop(token) }()
		return client, nil
	}

	headers := websocketHeaders(botID, token, false)
	dialer := websocket.Dialer{
		Proxy:            http.ProxyFromEnvironment,
		HandshakeTimeout: 45 * time.Second,
	}

	var conn *websocket.Conn
	var err error

	retryCount := 0
	for {
		mylog.Println("Dialing WebSocket endpoint:", safeEndpoint(urlStr))
		conn, _, err = dialer.Dial(urlStr, headers)
		if err != nil {
			retryCount++
			if retryCount > maxRetryAttempts {
				mylog.Printf("Exceeded maximum retry attempts for WebSocket[%s]", safeEndpoint(urlStr))
				return nil, errors.New("websocket connection failed")
			}
			mylog.Printf("Failed to connect to WebSocket[%s], retrying in 5 seconds...\n", safeEndpoint(urlStr))
			time.Sleep(5 * time.Second) // sleep for 5 seconds before retrying
		} else {
			mylog.Printf("Successfully connected to %s.\n", safeEndpoint(urlStr))
			break // successfully connected, break the loop
		}
	}
	client := &WebSocketClient{
		conn:      conn,
		botID:     botID,
		backendID: backendID,
		urlStr:    urlStr,
		writeCh:   make(chan writeRequest, 5000), // 缓冲区大小可以根据需求调整
		closeCh:   make(chan struct{}),
	}
	go client.startWriter() // 启动写 Goroutine

	// Sending initial message similar to your setupB function
	message := map[string]interface{}{
		"meta_event_type": "lifecycle",
		"post_type":       "meta_event",
		"self_id":         botID,
		"sub_type":        "connect",
		"time":            int(time.Now().Unix()),
	}

	mylog.Printf("Message: %+v\n", message)

	err = client.SendMessage(message)
	if err != nil {
		// handle error
		mylog.Printf("Error sending message: %v\n", err)
	}

	// Starting goroutine for heartbeats and another for listening to messages
	ctx, cancel := context.WithCancel(context.Background())

	client.cancel = cancel
	heartbeatinterval := config.GetHeartBeatInterval()
	go client.sendHeartbeat(ctx, botID, heartbeatinterval)
	go client.handleIncomingMessages(cancel)

	return client, nil
}

// bridgeConnectLoop keeps bridge-mode HTTP available while an application is
// starting or temporarily disconnected. Requests are never queued across a
// socket: the manager dispatches only against a currently registered socket.
func (client *WebSocketClient) bridgeConnectLoop(token string) {
	delays := []time.Duration{time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second, 30 * time.Second}
	dialer := websocket.Dialer{Proxy: http.ProxyFromEnvironment, HandshakeTimeout: 10 * time.Second}
	for attempt := 0; client.runCtx.Err() == nil; attempt++ {
		headers := websocketHeaders(client.botID, token, true)
		dialCtx, cancelDial := context.WithTimeout(client.runCtx, 12*time.Second)
		conn, _, err := dialer.DialContext(dialCtx, client.urlStr, headers)
		cancelDial()
		if err != nil {
			if client.runCtx.Err() != nil {
				return
			}
			mylog.Printf("OneBot websocket connection unavailable for %s; retrying", safeEndpoint(client.urlStr))
			if !client.waitReconnect(delays[min(attempt, len(delays)-1)]) {
				return
			}
			continue
		}

		client.connMu.Lock()
		old := client.conn
		client.conn = conn
		client.connMu.Unlock()
		if old != nil && old != conn {
			_ = old.Close()
		}
		mylog.Printf("OneBot websocket connected for %s", safeEndpoint(client.urlStr))
		connectEvent := map[string]interface{}{
			"meta_event_type": "lifecycle", "post_type": "meta_event",
			"self_id": client.botID, "sub_type": "connect", "time": int(time.Now().Unix()),
		}
		if err := client.sendMessageOnContext(client.runCtx, conn, connectEvent); err != nil {
			_ = conn.Close()
			client.clearConn(conn)
			if client.runCtx.Err() == nil && !client.waitReconnect(delays[0]) {
				return
			}
			continue
		}

		connectionCtx, cancelConnection := context.WithCancel(client.runCtx)
		client.connMu.Lock()
		client.cancel = cancelConnection
		client.connMu.Unlock()
		heartbeat := config.GetHeartBeatInterval()
		go client.sendHeartbeat(connectionCtx, client.botID, heartbeat)
		client.handleIncomingMessagesOn(conn, cancelConnection)
		cancelConnection()
		client.clearConn(conn)
		if client.runCtx.Err() != nil {
			return
		}
		mylog.Printf("OneBot websocket reconnecting for %s", safeEndpoint(client.urlStr))
		if !client.waitReconnect(delays[0]) {
			return
		}
	}
}

func (client *WebSocketClient) waitReconnect(delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-client.runCtx.Done():
		return false
	case <-client.closeCh:
		return false
	case <-timer.C:
		return true
	}
}

func (client *WebSocketClient) clearConn(conn *websocket.Conn) {
	client.connMu.Lock()
	if client.conn == conn {
		client.conn = nil
		client.cancel = nil
	}
	client.connMu.Unlock()
}

func websocketHeaders(botID uint64, token string, bearer bool) http.Header {
	headers := http.Header{
		"User-Agent":    []string{"CQHttp/4.15.0"},
		"X-Client-Role": []string{"Universal"},
		"X-Self-ID":     []string{fmt.Sprintf("%d", botID)},
	}
	if token != "" {
		scheme := "Token"
		if bearer {
			scheme = "Bearer"
		}
		headers.Set("Authorization", scheme+" "+token)
	}
	return headers
}

func safeEndpoint(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "<invalid websocket endpoint>"
	}
	return u.Scheme + "://" + u.Host + u.EscapedPath()
}

// getParamsFromURI 解析给定URI中的查询参数，并返回一个映射（map）
func getParamsFromURI(uriStr string) map[string]string {
	params := make(map[string]string)

	u, err := url.Parse(uriStr)
	if err != nil {
		mylog.Printf("Error parsing the URL: %v\n", err)
		return params
	}

	// 遍历查询参数并将其添加到返回的映射中
	for key, values := range u.Query() {
		if len(values) > 0 {
			params[key] = values[0] // 如果一个参数有多个值，这里只选择第一个。可以根据需求进行调整。
		}
	}

	return params
}

// respondToAction 根据action类型构造并发送响应消息
func (client *WebSocketClient) respondToAction(action string, echo interface{}, params callapi.ParamsContent) {
	client.connMu.RLock()
	conn := client.conn
	client.connMu.RUnlock()
	client.respondToActionOn(conn, action, echo, params)
}

func (client *WebSocketClient) respondToActionOn(conn *websocket.Conn, action string, echo interface{}, params callapi.ParamsContent) {
	var response map[string]interface{}

	switch action {
	case "get_group_list":
		response = make(map[string]interface{})
		data := make([]map[string]interface{}, 1) // 示例仅创建一个元素的数组
		for i := range data {
			data[i] = map[string]interface{}{
				"group_create_time": int64(0),
				"group_id":          wireID("868858989"),
				"group_level":       int64(0),
				"group_memo":        "",
				"group_name":        "可爱red",
				"max_member_count":  int64(3000),
				"member_count":      int64(1800),
			}
		}
		response["data"] = data
		response["message"] = ""
		response["retcode"] = 0
		response["status"] = "ok"
		response["echo"] = echo

	case "get_login_info":
		wxappidint := config.GetUinint64()
		response = map[string]interface{}{
			"data": map[string]interface{}{
				"nickname": "早苗",
				"user_id":  wxappidint,
			},
			"message": "",
			"retcode": 0,
			"status":  "ok",
			"echo":    echo,
		}

	case "get_group_info":
		groupID := params.GroupID
		if groupID == nil || fmt.Sprint(groupID) == "" {
			response = map[string]interface{}{"data": nil, "message": "group_id is required", "retcode": 1001, "status": "failed", "echo": echo}
		} else {
			wireGroupID := wireID(fmt.Sprint(groupID))
			if _, numeric := wireGroupID.(string); numeric && !config.GetStringOb11() {
				response = map[string]interface{}{"data": nil, "message": "group_id must be numeric", "retcode": 1001, "status": "failed", "echo": echo}
				break
			}
			response = map[string]interface{}{
				"data":    map[string]interface{}{"group_id": wireGroupID, "group_name": "fixture-group", "member_count": 0, "max_member_count": 0},
				"message": "", "retcode": 0, "status": "ok", "echo": echo,
			}
		}

	case "get_guild_service_profile":
		response = map[string]interface{}{
			"data": map[string]interface{}{
				"nickname": "",
				"tiny_id":  0,
			},
			"message": "",
			"retcode": 0,
			"status":  "ok",
			"echo":    echo,
		}

	case "get_online_clients":
		response = map[string]interface{}{
			"data": map[string]interface{}{
				"clients": []interface{}{}, // 创建一个空的clients数组
				"tiny_id": 0,
			},
			"message": "",
			"retcode": 0,
			"status":  "ok",
			"echo":    echo,
			"clients": []interface{}{}, // 根据描述，这可能是多余的，除非您有特定需求
		}

	case "get_version_info":
		response = map[string]interface{}{
			"data": map[string]interface{}{
				"app_full_name":              "go-cqhttp-v1.0.0_windows_amd64-go1.20.2",
				"app_name":                   "go-cqhttp",
				"app_version":                "v1.0.0",
				"coolq_directory":            "",
				"coolq_edition":              "pro",
				"go-cqhttp":                  true,
				"plugin_build_configuration": "release",
				"plugin_build_number":        99,
				"plugin_version":             "4.15.0",
				"protocol_name":              4,
				"protocol_version":           "v11",
				"runtime_os":                 "windows",
				"runtime_version":            "go1.20.2",
				"version":                    "v1.0.0",
			},
			"message": "",
			"retcode": 0,
			"status":  "ok",
			"echo":    echo,
		}

	case "get_friend_list":
		friends := []map[string]interface{}{
			{"nickname": "小狐狸", "remark": "", "user_id": wireID("2022717137")},
			// 添加更多好友信息...
		}
		response = map[string]interface{}{
			"data":    friends,
			"message": "",
			"retcode": 0,
			"status":  "ok",
			"echo":    echo,
		}

	case "get_guild_list":
		data := []map[string]interface{}{}
		// 假设我们要添加一个示例公会信息，实际应用中这部分可能需要从数据库或其他数据源动态获取
		for i := 0; i < 1; i++ { // 示例仅添加一个公会
			data = append(data, map[string]interface{}{
				"guild_id":         "0",         // 公会ID示例值
				"guild_name":       "868858989", // 公会名称示例值
				"guild_display_id": "868858989", // 公会显示ID示例值
			})
		}
		response = map[string]interface{}{
			"data":    data,
			"message": "",
			"retcode": 0,
			"status":  "ok",
			"echo":    echo,
		}

	case "get_guild_channel_list":
		// 这里示例不具体填充data数组中的频道信息，假设响应需要的是一个空的频道列表
		response = map[string]interface{}{
			"data":    []interface{}{}, // 创建一个空的频道列表
			"message": "",
			"retcode": 0,
			"status":  "ok",
			"echo":    echo,
		}

	default:
		response = map[string]interface{}{"data": nil, "message": "unsupported action", "retcode": 1404, "status": "failed", "echo": echo}
	}

	err := client.sendMessageOnContext(context.Background(), conn, response)
	if err != nil {
		mylog.Println("Error sending message:", err)
		return
	}

	mylog.Printf("Responded to OneBot action %q", action)
}

func wireID(raw string) interface{} {
	if config.GetStringOb11() {
		return raw
	}
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return raw
	}
	return id
}
