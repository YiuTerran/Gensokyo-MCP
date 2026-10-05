package bridge

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	bolt "go.etcd.io/bbolt"
)

const (
	QueueLimit           = 20 // waiting jobs per backend; the worker may own one more job.
	ExecutionDeadline    = 30 * time.Second
	MaxOutputsPerRequest = 64
	MaxOutputTextBytes   = 128 * 1024
	RequestTTL           = 24 * time.Hour
	PrivateOutboxTTL     = 10 * time.Minute
	identityIDStart      = int64(9_000_000_000_000_000)
	identityIDFloor      = int64(8_000_000_000_000_000)
)

var (
	ErrInvalidRequest     = errors.New("invalid bridge request")
	ErrQueueFull          = errors.New("backend request queue is full")
	ErrRequestConflict    = errors.New("request_id conflicts with a prior request")
	ErrBackendUnavailable = errors.New("backend is not registered")
	ErrConnectionChanged  = errors.New("backend connection changed")
	ErrBadCompletion      = errors.New("completion did not match accepted outputs")
	ErrAlreadyClaimed     = errors.New("private delivery has already been claimed")
	ErrNotFound           = errors.New("record not found")
	masterUserKeyPattern  = regexp.MustCompile(`^[0-9]{1,20}:[A-Za-z0-9_-]{1,128}$`)
)

type Request struct {
	BackendID string `json:"backend_id"`
	RequestID string `json:"request_id"`
	Audience  string `json:"audience"`
	Payload   string `json:"payload"`
	UserKey   string `json:"user_key,omitempty"`
	GroupKey  string `json:"group_key,omitempty"`
	UserID    int64  `json:"user_id,omitempty"`
	GroupID   int64  `json:"group_id,omitempty"`
}

type Output struct {
	Action   string `json:"action"`
	Audience string `json:"audience"`
	TargetID int64  `json:"target_id"`
	Message  string `json:"message"`
}

type Result struct {
	RequestID      string   `json:"request_id"`
	BackendID      string   `json:"backend_id"`
	Audience       string   `json:"audience"`
	Status         string   `json:"status"`
	Outputs        []Output `json:"outputs"`
	PrivateReceipt string   `json:"private_receipt,omitempty"`
	PrivateCount   int      `json:"private_count,omitempty"`
}

type RegisterParams struct {
	Version         int      `json:"version"`
	BackendInstance string   `json:"backend_instance"`
	Capabilities    []string `json:"capabilities"`
}

type BackendStatus struct {
	ID           string   `json:"id"`
	Ready        bool     `json:"ready"`
	Version      int      `json:"version"`
	Capabilities []string `json:"capabilities,omitempty"`
}

type Authorization struct {
	Version       int     `json:"version"`
	MasterUserIDs []int64 `json:"master_user_ids"`
}

type PrivateOutput struct {
	TargetID int64  `json:"target_id"`
	Message  string `json:"message"`
}

type PrivateDelivery struct {
	DeliveryID string          `json:"delivery_id"`
	Outputs    []PrivateOutput `json:"outputs"`
}

type diskRequest struct {
	Fingerprint     string `json:"fingerprint"`
	CreatedAt       int64  `json:"created_at"`
	State           string `json:"state"`
	Result          Result `json:"result"`
	BackendInstance string `json:"backend_instance,omitempty"`
	ConnectionID    string `json:"connection_id,omitempty"`
	SocketID        string `json:"socket_id,omitempty"`
	SourceID        int32  `json:"source_id,omitempty"`
	AcceptedCount   int    `json:"accepted_count,omitempty"`
	Receipt         string `json:"receipt,omitempty"`
}

type diskQuarantine struct {
	BackendInstance string `json:"backend_instance"`
	ConnectionID    string `json:"connection_id"`
	SocketID        string `json:"socket_id"`
	RequestKey      string `json:"request_key"`
	SourceID        int32  `json:"source_id"`
	AcceptedCount   int    `json:"accepted_count"`
}

type diskOutbox struct {
	BackendID  string          `json:"backend_id"`
	RequestID  string          `json:"request_id"`
	Receipt    string          `json:"receipt"`
	CreatedAt  int64           `json:"created_at"`
	Outputs    []PrivateOutput `json:"outputs"`
	Ready      bool            `json:"ready"`
	Claimed    bool            `json:"claimed"`
	DeliveryID string          `json:"delivery_id,omitempty"`
}

type callState struct {
	request         Request
	fingerprint     string
	requestID       int32
	connectionID    string
	socketID        string
	state           string
	backendInstance string
	deadline        time.Time
	outputs         []Output
	outputBytes     int
	acceptedCount   int
	private         []PrivateOutput
	receipt         string
	createdAt       int64
	done            chan struct{}
	result          Result
	err             error
}

type job struct {
	call     *callState
	ctx      context.Context
	cancel   context.CancelFunc
	dispatch func(context.Context, string, string, int32) error
}

type backend struct {
	status        BackendStatus
	socket        string
	conn          string
	instance      string
	authorization *Authorization
	quarantine    *diskQuarantine
	queue         chan job
}

type Manager struct {
	db               *bolt.DB
	mu               sync.Mutex
	backends         map[string]*backend
	active           map[string]*callState
	records          map[string]*callState
	quarantines      map[string]*diskQuarantine
	ctx              context.Context
	cancel           context.CancelFunc
	stop             chan struct{}
	workers          sync.WaitGroup
	closeOnce        sync.Once
	closeErr         error
	closing          bool
	dispatches       sync.WaitGroup
	readinessChanged func()
	masterUserKeys   []string
}

var (
	bucketMeta       = []byte("meta")
	bucketRequests   = []byte("requests")
	bucketIdentities = []byte("identities")
	bucketOutbox     = []byte("private_outbox")
	bucketQuarantine = []byte("quarantine")
)

func Open(path string) (*Manager, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		db: db, backends: map[string]*backend{}, active: map[string]*callState{},
		records: map[string]*callState{}, quarantines: map[string]*diskQuarantine{}, ctx: ctx, cancel: cancel, stop: make(chan struct{}),
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{bucketMeta, bucketRequests, bucketIdentities, bucketOutbox, bucketQuarantine} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}
		requests := tx.Bucket(bucketRequests)
		cursor := requests.Cursor()
		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			var record diskRequest
			if err := json.Unmarshal(value, &record); err != nil {
				return err
			}
			expired := time.Since(time.Unix(0, record.CreatedAt)) > RequestTTL
			if record.State == "dispatching" || record.State == "running" {
				quarantine := diskQuarantine{
					BackendInstance: record.BackendInstance, ConnectionID: record.ConnectionID,
					SocketID: record.SocketID, RequestKey: string(key), SourceID: record.SourceID,
					AcceptedCount: record.AcceptedCount,
				}
				if quarantine.BackendInstance == "" || quarantine.ConnectionID == "" || quarantine.SocketID == "" || quarantine.SourceID <= 0 {
					return errors.New("incomplete dispatched request record")
				}
				quarantineJSON, err := json.Marshal(quarantine)
				if err != nil {
					return err
				}
				if err := tx.Bucket(bucketQuarantine).Put([]byte(record.Result.BackendID), quarantineJSON); err != nil {
					return err
				}
				record.State = "unknown"
				record.Result.Status = "unknown"
				record.Result.Outputs = []Output{}
				record.Result.PrivateReceipt = ""
				record.Result.PrivateCount = 0
				unknownJSON, err := json.Marshal(record)
				if err != nil {
					return err
				}
				if err := requests.Put(key, unknownJSON); err != nil {
					return err
				}
				if record.Receipt != "" {
					if err := tx.Bucket(bucketOutbox).Delete(outboxKey(record.Result.BackendID, record.Result.RequestID, record.Receipt)); err != nil {
						return err
					}
				}
			} else if record.State == "queued" || record.State == "pending" {
				record.State = "unknown"
				record.Result.Status = "unknown"
				record.Result.Outputs = []Output{}
				record.Result.PrivateReceipt = ""
				record.Result.PrivateCount = 0
				encoded, err := json.Marshal(record)
				if err != nil {
					return err
				}
				if err := requests.Put(key, encoded); err != nil {
					return err
				}
			}
			if expired {
				if err := cursor.Delete(); err != nil {
					return err
				}
			}
		}
		return tx.Bucket(bucketQuarantine).ForEach(func(key, value []byte) error {
			var quarantine diskQuarantine
			if err := json.Unmarshal(value, &quarantine); err != nil {
				return err
			}
			copy := quarantine
			m.quarantines[string(key)] = &copy
			return nil
		})
	}); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := m.pruneOutbox(time.Now()); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketRequests).ForEach(func(key, value []byte) error {
			var record diskRequest
			if err := json.Unmarshal(value, &record); err != nil {
				return err
			}
			state := &callState{fingerprint: record.Fingerprint, state: "done", result: record.Result, createdAt: record.CreatedAt, done: make(chan struct{})}
			close(state.done)
			m.records[string(key)] = state
			return nil
		})
	}); err != nil {
		_ = db.Close()
		return nil, err
	}
	m.workers.Add(1)
	go m.cleanupLoop()
	return m, nil
}

func (m *Manager) Close() error {
	m.closeOnce.Do(func() {
		m.cancel()
		m.mu.Lock()
		m.closing = true
		for _, call := range m.records {
			if call.request.BackendID != "" && call.state != "done" {
				m.finishUnknownLocked(call, context.Canceled)
			}
		}
		close(m.stop)
		m.mu.Unlock()
		m.workers.Wait()
		dispatchesDone := make(chan struct{})
		go func() { m.dispatches.Wait(); close(dispatchesDone) }()
		select {
		case <-dispatchesDone:
		case <-time.After(time.Second):
		}
		m.closeErr = m.db.Close()
	})
	return m.closeErr
}

func (m *Manager) ConfigureBackends(ids []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, id := range ids {
		if id == "" || m.backends[id] != nil {
			continue
		}
		b := &backend{status: BackendStatus{ID: id}, queue: make(chan job, QueueLimit), quarantine: m.quarantines[id]}
		m.backends[id] = b
		m.workers.Add(1)
		go m.worker(id, b)
	}
}

func (m *Manager) SetReadinessCallback(callback func()) {
	m.mu.Lock()
	m.readinessChanged = callback
	m.mu.Unlock()
}

// SetMasterUserKeys configures stable source identities eligible for the
// negotiated bridge-only Master ACL. Keys are never included in status or logs.
func (m *Manager) SetMasterUserKeys(keys []string) {
	copyKeys := append([]string(nil), keys...)
	m.mu.Lock()
	m.masterUserKeys = copyKeys
	m.mu.Unlock()
}

// ParseMasterUserKeys reads a JSON string array of appId:originalSDKopenid
// keys. Invalid input is rejected as a whole so callers can fail closed;
// duplicate keys are collapsed while preserving their first occurrence.
func ParseMasterUserKeys(raw string) ([]string, bool) {
	if strings.TrimSpace(raw) == "" {
		return []string{}, true
	}
	var keys []string
	if err := json.Unmarshal([]byte(raw), &keys); err != nil || keys == nil || len(keys) > 100 {
		return []string{}, false
	}
	seen := make(map[string]struct{}, len(keys))
	unique := make([]string, 0, len(keys))
	for _, key := range keys {
		if !masterUserKeyPattern.MatchString(key) {
			return []string{}, false
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, key)
	}
	return unique, true
}

func (m *Manager) notifyReadinessLocked() {
	if callback := m.readinessChanged; callback != nil {
		go callback()
	}
}

func (m *Manager) Register(backendID, socketID string, params RegisterParams) (string, error) {
	if !bounded(backendID, 128) || !bounded(socketID, 128) || params.Version != 1 || !bounded(params.BackendInstance, 128) || !hasCapabilities(params.Capabilities) || !validCapabilities(params.Capabilities) {
		return "", ErrInvalidRequest
	}
	masterIDs := []int64{}
	if hasCapability(params.Capabilities, "master-acl-v1") {
		m.mu.Lock()
		keys := append([]string(nil), m.masterUserKeys...)
		m.mu.Unlock()
		for _, key := range keys {
			id, err := m.MapIdentity(backendID, "user", key)
			if err != nil {
				return "", err
			}
			masterIDs = append(masterIDs, id)
		}
	}
	connectionID, err := randomID()
	if err != nil {
		return "", err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	b := m.backends[backendID]
	if b == nil {
		return "", ErrBackendUnavailable
	}
	if m.closing {
		return "", ErrBackendUnavailable
	}
	old := b.conn
	oldInstance := b.instance
	if old != "" && b.socket == socketID {
		return "", ErrInvalidRequest
	}
	if quarantine := m.quarantines[backendID]; quarantine != nil && quarantine.BackendInstance != params.BackendInstance {
		if err := m.db.Update(func(tx *bolt.Tx) error { return tx.Bucket(bucketQuarantine).Delete([]byte(backendID)) }); err != nil {
			return "", err
		}
		delete(m.quarantines, backendID)
		b.quarantine = nil
	}
	newInstance := oldInstance != "" && oldInstance != params.BackendInstance
	if newInstance {
		// A different app process is the only implicit proof that the old task
		// cannot still be mutating this backend.
		b.quarantine = nil
		delete(m.quarantines, backendID)
		if err := m.db.Update(func(tx *bolt.Tx) error { return tx.Bucket(bucketQuarantine).Delete([]byte(backendID)) }); err != nil {
			return "", err
		}
	}
	b.conn, b.socket = connectionID, socketID
	b.instance = params.BackendInstance
	b.authorization = nil
	b.status.Capabilities = append([]string(nil), params.Capabilities...)
	if hasCapability(params.Capabilities, "master-acl-v1") {
		b.authorization = &Authorization{Version: 1, MasterUserIDs: append([]int64{}, masterIDs...)}
	}
	b.status.Ready, b.status.Version = false, 1
	m.notifyReadinessLocked()
	if old != "" {
		m.finishConnectionLocked(backendID, old, ErrConnectionChanged, newInstance)
	}
	return connectionID, nil
}

// Activate makes a registration available only after its OneBot success ACK has
// been written to the same socket that registered it.
func (m *Manager) Activate(backendID, socketID, connectionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	b := m.backends[backendID]
	if b == nil || b.socket != socketID || b.conn != connectionID {
		return ErrConnectionChanged
	}
	b.status.Ready = b.quarantine == nil
	m.notifyReadinessLocked()
	return nil
}

func hasCapabilities(capabilities []string) bool {
	seen := map[string]bool{}
	for _, capability := range capabilities {
		seen[capability] = true
	}
	return seen["reply"] && seen["complete"]
}

func hasCapability(capabilities []string, want string) bool {
	for _, capability := range capabilities {
		if capability == want {
			return true
		}
	}
	return false
}

func validCapabilities(capabilities []string) bool {
	if len(capabilities) > 32 {
		return false
	}
	seen := make(map[string]struct{}, len(capabilities))
	for _, capability := range capabilities {
		if !bounded(capability, 64) {
			return false
		}
		if _, exists := seen[capability]; exists {
			return false
		}
		seen[capability] = struct{}{}
	}
	return true
}

func (m *Manager) Disconnect(backendID, socketID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b := m.backends[backendID]
	if b == nil || b.socket != socketID {
		return
	}
	old := b.conn
	if call := m.active[backendID]; call != nil && call.connectionID == old {
		_ = m.finishUnknownLocked(call, ErrConnectionChanged)
	}
	b.conn, b.socket = "", ""
	b.status.Ready = false
	b.status.Capabilities = nil
	b.authorization = nil
	m.notifyReadinessLocked()
	m.finishConnectionLocked(backendID, old, ErrConnectionChanged, false)
}

// RegistrationAuthorization returns only the authorization negotiated by the
// exact currently registered connection. Replaced sockets cannot reuse it.
func (m *Manager) RegistrationAuthorization(backendID, connectionID string) (Authorization, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b := m.backends[backendID]
	if b == nil || connectionID == "" || b.conn != connectionID || b.authorization == nil {
		return Authorization{}, false
	}
	return Authorization{Version: b.authorization.Version, MasterUserIDs: append([]int64{}, b.authorization.MasterUserIDs...)}, true
}

func (m *Manager) finishConnectionLocked(backendID, connectionID string, err error, provenNewInstance bool) {
	for _, call := range m.records {
		if call.request.BackendID == backendID && call.connectionID == connectionID && call.state != "done" {
			if provenNewInstance {
				_ = m.finishStateLocked(call, "unknown", err, nil)
			} else {
				_ = m.finishUnknownLocked(call, err)
			}
		}
	}
}

func (m *Manager) Backends() []BackendStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	statuses := make([]BackendStatus, 0, len(m.backends))
	for _, b := range m.backends {
		status := b.status
		status.Capabilities = append([]string(nil), b.status.Capabilities...)
		statuses = append(statuses, status)
	}
	sort.Slice(statuses, func(i, j int) bool { return statuses[i].ID < statuses[j].ID })
	return statuses
}

func (m *Manager) MapIdentity(backendID, kind, key string) (int64, error) {
	if !bounded(backendID, 128) || !bounded(kind, 16) || !bounded(key, 512) || strings.ContainsAny(key, "\x00\r\n") {
		return 0, ErrInvalidRequest
	}
	lookup := []byte(backendID + "\x00" + kind + "\x00" + key)
	var id int64
	err := m.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketIdentities)
		if value := bucket.Get(lookup); value != nil {
			id = int64(binary.BigEndian.Uint64(value))
			return nil
		}
		meta := tx.Bucket(bucketMeta)
		sequence := readSequence(meta.Get([]byte("identity-sequence"))) + 1
		if sequence == 0 || sequence >= uint64(identityIDStart-identityIDFloor) {
			return errors.New("identity allocation exhausted")
		}
		id = identityIDStart - int64(sequence)
		if err := bucket.Put(lookup, sequenceBytes(uint64(id))); err != nil {
			return err
		}
		return meta.Put([]byte("identity-sequence"), sequenceBytes(sequence))
	})
	return id, err
}

func (m *Manager) NextMessageID() (int32, error) {
	var id uint64
	err := m.db.Update(func(tx *bolt.Tx) error {
		meta := tx.Bucket(bucketMeta)
		id = readSequence(meta.Get([]byte("message-sequence"))) + 1
		if id == 0 || id > math.MaxInt32 {
			return errors.New("OneBot message ID space exhausted")
		}
		return meta.Put([]byte("message-sequence"), sequenceBytes(id))
	})
	return int32(id), err
}

func (m *Manager) Call(ctx context.Context, request Request, dispatch func(context.Context, string, string, int32) error) (Result, error) {
	if !validRequest(request) || dispatch == nil {
		return Result{}, ErrInvalidRequest
	}
	key := requestKey(request.BackendID, request.RequestID)
	fingerprint := fingerprintRequest(request)
	_ = m.prune(time.Now())
	m.mu.Lock()
	if m.closing {
		m.mu.Unlock()
		return Result{}, ErrBackendUnavailable
	}
	if existing := m.records[key]; existing != nil {
		if existing.fingerprint != fingerprint {
			m.mu.Unlock()
			return Result{}, ErrRequestConflict
		}
		m.mu.Unlock()
		return waitCall(ctx, existing)
	}
	b := m.backends[request.BackendID]
	if b == nil || !b.status.Ready || b.conn == "" || b.quarantine != nil {
		m.mu.Unlock()
		return Result{}, ErrBackendUnavailable
	}
	if len(b.queue) >= cap(b.queue) {
		m.mu.Unlock()
		return Result{}, ErrQueueFull
	}
	messageID, err := m.nextMessageID()
	if err != nil {
		m.mu.Unlock()
		return Result{}, err
	}
	receipt, err := randomID()
	if err != nil {
		m.mu.Unlock()
		return Result{}, err
	}
	created := time.Now()
	createdAt := created.UnixNano()
	callCtx, cancel := context.WithDeadline(ctx, created.Add(ExecutionDeadline))
	call := &callState{
		request: request, fingerprint: fingerprint, requestID: messageID,
		connectionID: b.conn, socketID: b.socket, backendInstance: b.instance, state: "queued", receipt: receipt,
		createdAt: createdAt, deadline: created.Add(ExecutionDeadline), done: make(chan struct{}),
		result: Result{RequestID: request.RequestID, BackendID: request.BackendID, Audience: request.Audience, Status: "unknown", Outputs: []Output{}},
	}
	record := m.diskRecord(call, "queued")
	if err := m.writeRequest(key, record); err != nil {
		cancel()
		m.mu.Unlock()
		return Result{}, err
	}
	m.records[key] = call
	select {
	case b.queue <- job{call: call, ctx: callCtx, cancel: cancel, dispatch: dispatch}:
		m.mu.Unlock()
	default:
		cancel()
		delete(m.records, key)
		_ = m.deleteRequest(key)
		m.mu.Unlock()
		return Result{}, ErrQueueFull
	}
	go func() {
		select {
		case <-callCtx.Done():
			m.cancelCall(request.BackendID, call, callCtx.Err())
		case <-call.done:
		}
	}()
	result, waitErr := waitCall(callCtx, call)
	cancel()
	return result, waitErr
}

func validRequest(request Request) bool {
	if !bounded(request.BackendID, 128) || !bounded(request.RequestID, 128) || len(request.Payload) > 16*1024 || utf8.RuneCountInString(request.Payload) > 4000 || request.Payload == "" || strings.ContainsAny(request.BackendID+request.RequestID+request.Payload, "\x00\r\n") || (request.Audience != "group" && request.Audience != "private") {
		return false
	}
	identity := request.UserKey != ""
	if identity {
		if !bounded(request.UserKey, 512) || strings.ContainsAny(request.UserKey, "\x00\r\n") || request.UserID < identityIDFloor || request.UserID >= identityIDStart {
			return false
		}
	} else if request.UserKey != "" || request.UserID <= 0 || request.UserID >= identityIDFloor || request.UserID > 9_007_199_254_740_991 {
		return false
	}
	if request.Audience == "group" {
		if identity {
			return bounded(request.GroupKey, 512) && !strings.ContainsAny(request.GroupKey, "\x00\r\n") && request.GroupID >= identityIDFloor && request.GroupID < identityIDStart
		}
		return request.GroupKey == "" && request.GroupID > 0 && request.GroupID < identityIDFloor
	}
	return request.GroupKey == "" && request.GroupID == 0
}

func waitCall(ctx context.Context, call *callState) (Result, error) {
	select {
	case <-call.done:
		return call.result, call.err
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}
}

func (m *Manager) worker(backendID string, b *backend) {
	defer m.workers.Done()
	for {
		select {
		case <-m.stop:
			return
		case queued := <-b.queue:
			m.execute(backendID, queued)
		}
	}
}

func (m *Manager) execute(backendID string, queued job) {
	defer queued.cancel()
	call := queued.call
	m.mu.Lock()
	b := m.backends[backendID]
	if call.state != "queued" || m.closing {
		m.mu.Unlock()
		return
	}
	if err := queued.ctx.Err(); err != nil {
		_ = m.finishUnknownLocked(call, err)
		m.mu.Unlock()
		return
	}
	if b == nil || !b.status.Ready || b.quarantine != nil || b.conn != call.connectionID || b.socket != call.socketID || b.instance != call.backendInstance {
		_ = m.finishUnknownLocked(call, ErrConnectionChanged)
		m.mu.Unlock()
		return
	}
	call.state = "dispatching"
	if err := m.writeRequest(requestKey(backendID, call.request.RequestID), m.diskRecord(call, "dispatching")); err != nil {
		call.state = "queued"
		_ = m.finishUnknownLocked(call, err)
		m.mu.Unlock()
		return
	}
	m.active[backendID] = call
	m.dispatches.Add(1)
	m.mu.Unlock()
	dispatched := make(chan error, 1)
	go func() {
		defer m.dispatches.Done()
		dispatched <- queued.dispatch(queued.ctx, call.connectionID, call.socketID, call.requestID)
	}()
	select {
	case <-call.done:
		return
	case err := <-dispatched:
		if err != nil {
			m.mu.Lock()
			_ = m.finishUnknownLocked(call, errors.New("dispatch failed"))
			m.mu.Unlock()
			return
		}
	case <-queued.ctx.Done():
		m.mu.Lock()
		_ = m.finishUnknownLocked(call, queued.ctx.Err())
		m.mu.Unlock()
		return
	}
	select {
	case <-call.done:
	case <-queued.ctx.Done():
		m.mu.Lock()
		_ = m.finishUnknownLocked(call, queued.ctx.Err())
		m.mu.Unlock()
	}
}

func (m *Manager) Deliver(backendID, connectionID string, sourceID int32, action, audience string, targetID int64, message string) error {
	if sourceID <= 0 || targetID <= 0 || (audience != "group" && audience != "private") || !bounded(message, MaxOutputTextBytes) {
		return ErrInvalidRequest
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	b, call := m.backends[backendID], m.active[backendID]
	if b == nil || call == nil || b.conn != connectionID || call.connectionID != connectionID || call.requestID != sourceID || call.state != "dispatching" {
		return ErrConnectionChanged
	}
	if audience == "group" && (call.request.Audience != "group" || call.request.GroupID != targetID) {
		return ErrInvalidRequest
	}
	if audience == "private" && call.request.UserID != targetID {
		return ErrInvalidRequest
	}
	if len(call.outputs)+len(call.private) >= MaxOutputsPerRequest || call.outputBytes+len(message) > MaxOutputTextBytes {
		return ErrInvalidRequest
	}
	output := PrivateOutput{TargetID: targetID, Message: message}
	var privateOutput *PrivateOutput
	if audience == "private" && call.request.Audience == "group" {
		privateOutput = &output
	}
	if err := m.persistAcceptedOutput(call, privateOutput); err != nil {
		return err
	}
	if privateOutput != nil {
		call.private = append(call.private, output)
	} else {
		call.outputs = append(call.outputs, Output{Action: action, Audience: audience, TargetID: targetID, Message: message})
	}
	call.outputBytes += len(message)
	call.acceptedCount++
	return nil
}

func (m *Manager) Complete(backendID, connectionID string, sourceID int32, status string, outputCount int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	call, b := m.active[backendID], m.backends[backendID]
	if quarantine := m.quarantines[backendID]; quarantine != nil && quarantine.ConnectionID == connectionID && quarantine.SourceID == sourceID {
		if (status != "ok" && status != "failed") || outputCount != quarantine.AcceptedCount {
			return ErrBadCompletion
		}
		if err := m.db.Update(func(tx *bolt.Tx) error { return tx.Bucket(bucketQuarantine).Delete([]byte(backendID)) }); err != nil {
			return err
		}
		delete(m.quarantines, backendID)
		b.quarantine = nil
		if b.conn == connectionID && b.socket == quarantine.SocketID && b.instance == quarantine.BackendInstance {
			b.status.Ready = true
		}
		m.notifyReadinessLocked()
		return nil
	}
	if call == nil || b == nil || call.state != "dispatching" || call.connectionID != connectionID || b.conn != connectionID || call.requestID != sourceID {
		return ErrConnectionChanged
	}
	accepted := call.acceptedCount
	if (status != "ok" && status != "failed") || outputCount < 0 || outputCount != accepted {
		_ = m.finishUnknownLocked(call, ErrBadCompletion)
		return ErrBadCompletion
	}
	if accepted > MaxOutputsPerRequest || call.outputBytes > MaxOutputTextBytes {
		_ = m.finishUnknownLocked(call, ErrBadCompletion)
		return ErrBadCompletion
	}
	return m.finishLocked(backendID, call, status, nil)
}

func (m *Manager) finishLocked(backendID string, call *callState, status string, callErr error) error {
	return m.finishStateLocked(call, status, callErr, nil)
}

func (m *Manager) finishUnknownLocked(call *callState, callErr error) error {
	var quarantine *diskQuarantine
	if call.state == "dispatching" {
		quarantine = &diskQuarantine{BackendInstance: call.backendInstance, ConnectionID: call.connectionID, SocketID: call.socketID, RequestKey: requestKey(call.request.BackendID, call.request.RequestID), SourceID: call.requestID, AcceptedCount: call.acceptedCount}
	}
	return m.finishStateLocked(call, "unknown", callErr, quarantine)
}

func (m *Manager) finishStateLocked(call *callState, status string, callErr error, quarantine *diskQuarantine) error {
	if call.state == "done" {
		return call.err
	}
	backendID := call.request.BackendID
	result := Result{RequestID: call.request.RequestID, BackendID: backendID, Audience: call.request.Audience, Status: status, Outputs: []Output{}}
	if status != "unknown" {
		for _, output := range call.outputs {
			if call.request.Audience == "private" || output.Audience == "group" {
				result.Outputs = append(result.Outputs, output)
			}
		}
		if status == "ok" && call.request.Audience == "group" && len(call.private) > 0 {
			result.PrivateReceipt, result.PrivateCount = call.receipt, len(call.private)
		}
	}
	key := requestKey(backendID, call.request.RequestID)
	record := m.diskRecord(call, status)
	record.Result = result
	err := m.db.Update(func(tx *bolt.Tx) error {
		encoded, err := json.Marshal(record)
		if err != nil {
			return err
		}
		if err := tx.Bucket(bucketRequests).Put([]byte(key), encoded); err != nil {
			return err
		}
		outbox := tx.Bucket(bucketOutbox)
		privateKey := outboxKey(backendID, call.request.RequestID, call.receipt)
		if status != "ok" || len(call.private) == 0 {
			if err := outbox.Delete(privateKey); err != nil {
				return err
			}
		} else {
			raw := outbox.Get(privateKey)
			if raw == nil {
				return errors.New("private output record is missing")
			}
			var privateRecord diskOutbox
			if err := json.Unmarshal(raw, &privateRecord); err != nil {
				return err
			}
			privateRecord.Ready = true
			privateEncoded, err := json.Marshal(privateRecord)
			if err != nil {
				return err
			}
			if err := outbox.Put(privateKey, privateEncoded); err != nil {
				return err
			}
		}
		qbucket := tx.Bucket(bucketQuarantine)
		if quarantine == nil {
			// A terminal queued request or ordinary completion cannot prove that
			// some other dispatched request has drained. Preserve any existing
			// backend quarantine until Register(new instance) or exact Complete.
			if current := qbucket.Get([]byte(backendID)); current != nil {
				return nil
			}
			return nil
		}
		qencoded, err := json.Marshal(quarantine)
		if err != nil {
			return err
		}
		return qbucket.Put([]byte(backendID), qencoded)
	})
	if err != nil {
		callErr = errors.New("bridge state persistence failed")
		result = Result{RequestID: call.request.RequestID, BackendID: backendID, Audience: call.request.Audience, Status: "unknown", Outputs: []Output{}}
		if quarantine == nil && call.state == "dispatching" {
			quarantine = &diskQuarantine{BackendInstance: call.backendInstance, ConnectionID: call.connectionID, SocketID: call.socketID, RequestKey: key, SourceID: call.requestID, AcceptedCount: call.acceptedCount}
		}
		fallback := m.diskRecord(call, "unknown")
		fallback.Result = result
		_ = m.db.Update(func(tx *bolt.Tx) error {
			encoded, encodeErr := json.Marshal(fallback)
			if encodeErr != nil {
				return encodeErr
			}
			if putErr := tx.Bucket(bucketRequests).Put([]byte(key), encoded); putErr != nil {
				return putErr
			}
			if deleteErr := tx.Bucket(bucketOutbox).Delete(outboxKey(backendID, call.request.RequestID, call.receipt)); deleteErr != nil {
				return deleteErr
			}
			if quarantine != nil {
				qencoded, qerr := json.Marshal(quarantine)
				if qerr != nil {
					return qerr
				}
				return tx.Bucket(bucketQuarantine).Put([]byte(backendID), qencoded)
			}
			return nil
		})
	}
	call.err, call.result, call.state = callErr, result, "done"
	if err != nil {
		call.err = errors.New("bridge state persistence failed")
	}
	if m.active[backendID] == call {
		delete(m.active, backendID)
	}
	if quarantine != nil {
		copy := *quarantine
		m.quarantines[backendID] = &copy
		if b := m.backends[backendID]; b != nil {
			b.quarantine, b.status.Ready = &copy, false
		}
		m.notifyReadinessLocked()
	} else if current := m.quarantines[backendID]; current != nil {
		if b := m.backends[backendID]; b != nil {
			b.quarantine, b.status.Ready = current, false
		}
	}
	close(call.done)
	return call.err
}

func (m *Manager) cancelCall(backendID string, call *callState, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if call.state != "done" {
		_ = m.finishUnknownLocked(call, err)
	}
}

func (m *Manager) ClaimPrivate(backendID, requestID, receipt string) (PrivateDelivery, error) {
	if !bounded(backendID, 128) || !bounded(requestID, 128) || !bounded(receipt, 64) {
		return PrivateDelivery{}, ErrInvalidRequest
	}
	var result PrivateDelivery
	err := m.db.Update(func(tx *bolt.Tx) error {
		requestRaw := tx.Bucket(bucketRequests).Get([]byte(requestKey(backendID, requestID)))
		if requestRaw == nil {
			return ErrNotFound
		}
		var request diskRequest
		if err := json.Unmarshal(requestRaw, &request); err != nil {
			return err
		}
		if request.State != "ok" || request.Result.PrivateReceipt != receipt || time.Since(time.Unix(0, request.CreatedAt)) > RequestTTL {
			return ErrNotFound
		}
		bucket := tx.Bucket(bucketOutbox)
		key := outboxKey(backendID, requestID, receipt)
		raw := bucket.Get(key)
		if raw == nil {
			return ErrNotFound
		}
		var record diskOutbox
		if err := json.Unmarshal(raw, &record); err != nil {
			return err
		}
		if time.Since(time.Unix(0, record.CreatedAt)) > PrivateOutboxTTL || !record.Ready {
			return ErrNotFound
		}
		if record.Claimed {
			return ErrAlreadyClaimed
		}
		deliveryID, err := randomID()
		if err != nil {
			return err
		}
		record.Claimed, record.DeliveryID = true, deliveryID
		encoded, err := json.Marshal(record)
		if err != nil {
			return err
		}
		if err := bucket.Put(key, encoded); err != nil {
			return err
		}
		result = PrivateDelivery{DeliveryID: deliveryID, Outputs: append([]PrivateOutput(nil), record.Outputs...)}
		return nil
	})
	return result, err
}

func (m *Manager) AckPrivate(deliveryID, status string) error {
	if !bounded(deliveryID, 64) || (status != "sent" && status != "failed" && status != "unknown") {
		return ErrInvalidRequest
	}
	return m.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketOutbox)
		cursor := bucket.Cursor()
		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			var record diskOutbox
			if err := json.Unmarshal(value, &record); err != nil {
				continue
			}
			if record.DeliveryID == deliveryID {
				return cursor.Delete() // every receipt status is final; plaintext is removed.
			}
		}
		return ErrNotFound
	})
}

func (m *Manager) appendPrivate(backendID, requestID, receipt string, output PrivateOutput) error {
	return m.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketOutbox)
		key := outboxKey(backendID, requestID, receipt)
		var record diskOutbox
		if raw := bucket.Get(key); raw != nil {
			if err := json.Unmarshal(raw, &record); err != nil {
				return err
			}
			if record.Claimed || record.Ready {
				return ErrAlreadyClaimed
			}
		} else {
			record = diskOutbox{BackendID: backendID, RequestID: requestID, Receipt: receipt, CreatedAt: time.Now().UnixNano()}
		}
		record.Outputs = append(record.Outputs, output)
		encoded, err := json.Marshal(record)
		if err != nil {
			return err
		}
		return bucket.Put(key, encoded)
	})
}

func (m *Manager) diskRecord(call *callState, state string) diskRequest {
	return diskRequest{
		Fingerprint: call.fingerprint, CreatedAt: call.createdAt, State: state, Result: call.result,
		BackendInstance: call.backendInstance, ConnectionID: call.connectionID, SocketID: call.socketID,
		SourceID: call.requestID, AcceptedCount: call.acceptedCount, Receipt: call.receipt,
	}
}

func (m *Manager) persistAcceptedOutput(call *callState, privateOutput *PrivateOutput) error {
	key := requestKey(call.request.BackendID, call.request.RequestID)
	return m.db.Update(func(tx *bolt.Tx) error {
		requests := tx.Bucket(bucketRequests)
		raw := requests.Get([]byte(key))
		if raw == nil {
			return ErrNotFound
		}
		var record diskRequest
		if err := json.Unmarshal(raw, &record); err != nil {
			return err
		}
		if record.State != "dispatching" || record.ConnectionID != call.connectionID || record.SourceID != call.requestID {
			return ErrConnectionChanged
		}
		record.AcceptedCount = call.acceptedCount + 1
		encoded, err := json.Marshal(record)
		if err != nil {
			return err
		}
		if err := requests.Put([]byte(key), encoded); err != nil {
			return err
		}
		if privateOutput == nil {
			return nil
		}
		outbox := tx.Bucket(bucketOutbox)
		outKey := outboxKey(call.request.BackendID, call.request.RequestID, call.receipt)
		var privateRecord diskOutbox
		if existing := outbox.Get(outKey); existing != nil {
			if err := json.Unmarshal(existing, &privateRecord); err != nil {
				return err
			}
			if privateRecord.Claimed || privateRecord.Ready {
				return ErrAlreadyClaimed
			}
		} else {
			privateRecord = diskOutbox{BackendID: call.request.BackendID, RequestID: call.request.RequestID, Receipt: call.receipt, CreatedAt: time.Now().UnixNano()}
		}
		privateRecord.Outputs = append(privateRecord.Outputs, *privateOutput)
		privateJSON, err := json.Marshal(privateRecord)
		if err != nil {
			return err
		}
		return outbox.Put(outKey, privateJSON)
	})
}

func (m *Manager) nextMessageID() (int32, error) {
	var id uint64
	err := m.db.Update(func(tx *bolt.Tx) error {
		meta := tx.Bucket(bucketMeta)
		id = readSequence(meta.Get([]byte("message-sequence"))) + 1
		if id == 0 || id > math.MaxInt32 {
			return errors.New("OneBot message ID space exhausted")
		}
		return meta.Put([]byte("message-sequence"), sequenceBytes(id))
	})
	return int32(id), err
}

func (m *Manager) writeRequest(key string, record diskRequest) error {
	encoded, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return m.db.Update(func(tx *bolt.Tx) error { return tx.Bucket(bucketRequests).Put([]byte(key), encoded) })
}

func (m *Manager) deleteRequest(key string) error {
	return m.db.Update(func(tx *bolt.Tx) error { return tx.Bucket(bucketRequests).Delete([]byte(key)) })
}

func (m *Manager) cleanupLoop() {
	defer m.workers.Done()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-m.stop:
			return
		case now := <-ticker.C:
			_ = m.prune(now)
		}
	}
}

func (m *Manager) prune(now time.Time) error {
	if err := m.db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{bucketRequests, bucketOutbox} {
			bucket := tx.Bucket(name)
			cursor := bucket.Cursor()
			for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
				var createdAt int64
				if name == nil {
					return errors.New("invalid bucket")
				}
				if name[0] == 'r' {
					var record diskRequest
					if err := json.Unmarshal(value, &record); err != nil {
						if err := cursor.Delete(); err != nil {
							return err
						}
						continue
					}
					createdAt = record.CreatedAt
					if now.Sub(time.Unix(0, createdAt)) > RequestTTL {
						if err := cursor.Delete(); err != nil {
							return err
						}
					}
				} else {
					var record diskOutbox
					if err := json.Unmarshal(value, &record); err != nil {
						if err := cursor.Delete(); err != nil {
							return err
						}
						continue
					}
					createdAt = record.CreatedAt
					if now.Sub(time.Unix(0, createdAt)) > PrivateOutboxTTL {
						if err := cursor.Delete(); err != nil {
							return err
						}
					}
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	m.mu.Lock()
	for key, call := range m.records {
		if now.Sub(time.Unix(0, call.createdAt)) > RequestTTL {
			delete(m.records, key)
		}
	}
	m.mu.Unlock()
	return nil
}

func (m *Manager) pruneOutbox(now time.Time) error {
	return m.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketOutbox)
		cursor := bucket.Cursor()
		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			var record diskOutbox
			if err := json.Unmarshal(value, &record); err != nil || now.Sub(time.Unix(0, record.CreatedAt)) > PrivateOutboxTTL {
				if err := cursor.Delete(); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func requestKey(backendID, requestID string) string { return backendID + "\x00" + requestID }
func outboxKey(backendID, requestID, receipt string) []byte {
	return []byte(backendID + "\x00" + requestID + "\x00" + receipt)
}
func fingerprintRequest(request Request) string {
	data, _ := json.Marshal(request)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
func randomID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}
func bounded(value string, max int) bool { return value != "" && len(value) <= max }
func readSequence(value []byte) uint64 {
	if len(value) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(value)
}
func sequenceBytes(value uint64) []byte {
	result := make([]byte, 8)
	binary.BigEndian.PutUint64(result, value)
	return result
}

// DecodeJSONNumber preserves exact OneBot IDs and never routes through float64.
func DecodeJSONNumber(raw json.RawMessage) (int64, error) {
	number := strings.TrimSpace(string(raw))
	if number == "" {
		return 0, fmt.Errorf("invalid numeric ID")
	}
	for _, digit := range number {
		if digit < '0' || digit > '9' {
			return 0, fmt.Errorf("invalid numeric ID")
		}
	}
	value, err := strconv.ParseInt(number, 10, 64)
	if err != nil || value <= 0 || value > 9_007_199_254_740_991 {
		return 0, fmt.Errorf("invalid numeric ID")
	}
	return value, nil
}
