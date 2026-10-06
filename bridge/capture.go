package bridge

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	bolt "go.etcd.io/bbolt"
)

const captureAckTimeout = 30 * time.Second

func dispatchOrderKey(backendID string) []byte { return []byte("dispatch-order\x00" + backendID) }

func (m *Manager) reserveDispatchOrder(backendID string) (uint64, error) {
	var order uint64
	err := m.db.Update(func(tx *bolt.Tx) error {
		var err error
		order, err = reserveDispatchOrderTx(tx, backendID)
		return err
	})
	return order, err
}

func reserveDispatchOrderTx(tx *bolt.Tx, backendID string) (uint64, error) {
	meta := tx.Bucket(bucketMeta)
	key := dispatchOrderKey(backendID)
	order := readSequence(meta.Get(key)) + 1
	if order == 0 {
		return 0, errors.New("bridge dispatch sequence exhausted")
	}
	return order, meta.Put(key, sequenceBytes(order))
}

func (m *Manager) Capture(event LogEvent) error {
	if !validLogEvent(event) {
		return ErrInvalidRequest
	}
	kind := "user"
	if event.IsBot {
		kind = "bot"
	}
	groupID, err := m.MapIdentity(event.BackendID, "group", event.GroupKey)
	if err != nil {
		return ErrCaptureStateUnavailable
	}
	userID, err := m.MapIdentity(event.BackendID, kind, event.UserKey)
	if err != nil {
		return ErrCaptureStateUnavailable
	}
	if event.Kind == "gap" {
		event.Nickname = ""
		event.Text = fixedGapText(1, event.Time, event.Time)
	}
	fingerprint := fingerprintLogEvent(event)
	key := logEventKey(event.BackendID, event.EventID)
	m.mu.Lock()
	b := m.backends[event.BackendID]
	if b == nil || !b.captureEnabled {
		m.mu.Unlock()
		return ErrCapabilityUnsupported
	}
	var queueFull bool
	err = m.db.Update(func(tx *bolt.Tx) error {
		seen := tx.Bucket(bucketLogSeen)
		rejected := tx.Bucket(bucketLogRejected)
		if raw := seen.Get([]byte(key)); raw != nil {
			var prior diskLogSeen
			if err := json.Unmarshal(raw, &prior); err != nil {
				return err
			}
			if prior.Fingerprint != fingerprint {
				return ErrRequestConflict
			}
			return nil
		}
		if raw := rejected.Get([]byte(key)); raw != nil {
			var prior diskLogSeen
			if err := json.Unmarshal(raw, &prior); err != nil {
				return err
			}
			if prior.Fingerprint != fingerprint {
				return ErrRequestConflict
			}
			queueFull = true
			return nil
		}
		pending := tx.Bucket(bucketLogEvents)
		count, usedBytes, gapCount := 0, 0, 0
		if err := pending.ForEach(func(_, value []byte) error {
			var record diskLogEvent
			if err := json.Unmarshal(value, &record); err != nil {
				return err
			}
			if record.Event.BackendID != event.BackendID {
				return nil
			}
			usedBytes += record.QueueBytes
			if record.GapCount != 0 {
				gapCount++
			} else {
				count++
			}
			return nil
		}); err != nil {
			return err
		}
		full := count >= CaptureEventLimit || usedBytes+logEventBytes(event) > CaptureQueueBytes
		if !full {
			seenCount := 0
			seenPrefix := event.BackendID + "\x00"
			if err := seen.ForEach(func(seenKey, _ []byte) error {
				if strings.HasPrefix(string(seenKey), seenPrefix) {
					seenCount++
				}
				return nil
			}); err != nil {
				return err
			}
			full = seenCount >= CaptureSeenLimit
		}
		if full {
			queueFull = true
			rejectedCount := 0
			rejectedPrefix := event.BackendID + "\x00"
			if err := rejected.ForEach(func(rejectedKey, _ []byte) error {
				if strings.HasPrefix(string(rejectedKey), rejectedPrefix) {
					rejectedCount++
				}
				return nil
			}); err != nil {
				return err
			}
			if rejectedCount >= CaptureRejectedLimit {
				return ErrCaptureStateUnavailable
			}
			gapEventID, err := persistOverflowGap(tx, event, groupID, userID, gapCount)
			if err != nil {
				return err
			}
			rejectedValue, err := json.Marshal(diskLogSeen{Fingerprint: fingerprint, CreatedAt: time.Now().UnixNano(), BackendID: event.BackendID, GapEventID: gapEventID})
			if err != nil {
				return err
			}
			return rejected.Put([]byte(key), rejectedValue)
		}
		order, err := reserveDispatchOrderTx(tx, event.BackendID)
		if err != nil {
			return err
		}
		record := diskLogEvent{Event: event, GroupID: groupID, UserID: userID, Order: order, CreatedAt: time.Now().UnixNano(), QueueBytes: logEventBytes(event)}
		encoded, err := json.Marshal(record)
		if err != nil {
			return err
		}
		if err := pending.Put([]byte(key), encoded); err != nil {
			return err
		}
		seenValue, err := json.Marshal(diskLogSeen{Fingerprint: fingerprint, CreatedAt: time.Now().UnixNano(), BackendID: event.BackendID})
		if err != nil {
			return err
		}
		if err := seen.Put([]byte(key), seenValue); err != nil {
			return err
		}
		return preventGroupGapMergeTx(tx, event.BackendID, event.GroupKey, order)
	})
	wakeLogWorker(b)
	m.mu.Unlock()
	if err != nil {
		if !errors.Is(err, ErrRequestConflict) && !errors.Is(err, ErrCapabilityUnsupported) {
			return ErrCaptureStateUnavailable
		}
		return err
	}
	if queueFull {
		return ErrCaptureQueueFull
	}
	return nil
}

func preventGroupGapMergeTx(tx *bolt.Tx, backendID, groupKey string, order uint64) error {
	if backendID == "" || groupKey == "" || order == 0 {
		return nil
	}
	bucket := tx.Bucket(bucketLogEvents)
	cursor := bucket.Cursor()
	for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
		var record diskLogEvent
		if err := json.Unmarshal(value, &record); err != nil {
			return err
		}
		if record.Event.BackendID != backendID || record.Event.GroupKey != groupKey || record.Order >= order || record.GapCount == 0 || !record.Mergeable {
			continue
		}
		record.Mergeable = false
		encoded, err := json.Marshal(record)
		if err != nil {
			return err
		}
		if err := bucket.Put(key, encoded); err != nil {
			return err
		}
	}
	return nil
}

func persistOverflowGap(tx *bolt.Tx, source LogEvent, groupID, userID int64, gapCount int) (string, error) {
	bucket := tx.Bucket(bucketLogEvents)
	var existingKey []byte
	var existing diskLogEvent
	if err := bucket.ForEach(func(key, value []byte) error {
		var record diskLogEvent
		if err := json.Unmarshal(value, &record); err != nil {
			return err
		}
		if record.Event.BackendID == source.BackendID && record.Event.GroupKey == source.GroupKey && record.GapCount != 0 && record.Mergeable && record.InflightConn == "" && record.Attempts == 0 {
			existingKey = append([]byte(nil), key...)
			existing = record
		}
		return nil
	}); err != nil {
		return "", err
	}
	if existingKey != nil {
		existing.GapCount++
		if existing.GapFirstTime == 0 || source.Time < existing.GapFirstTime {
			existing.GapFirstTime = source.Time
		}
		if source.Time > existing.GapLastTime {
			existing.GapLastTime = source.Time
		}
		existing.Event.Time = existing.GapLastTime
		existing.Event.Text = fixedGapText(existing.GapCount, existing.GapFirstTime, existing.GapLastTime)
		existing.QueueBytes = logEventBytes(existing.Event)
		encoded, err := json.Marshal(existing)
		if err != nil {
			return "", err
		}
		if err := bucket.Put(existingKey, encoded); err != nil {
			return "", err
		}
		return existing.Event.EventID, nil
	}
	if gapCount >= CaptureGapLimit {
		return "", ErrCaptureStateUnavailable
	}
	order, err := reserveDispatchOrderTx(tx, source.BackendID)
	if err != nil {
		return "", err
	}
	random, err := randomID()
	if err != nil {
		return "", err
	}
	event := LogEvent{BackendID: source.BackendID, EventID: "gap-" + random, GroupKey: source.GroupKey, UserKey: source.UserKey, Time: source.Time, IsBot: source.IsBot, Kind: "gap", Text: fixedGapText(1, source.Time, source.Time)}
	record := diskLogEvent{Event: event, GroupID: groupID, UserID: userID, Order: order, CreatedAt: time.Now().UnixNano(), QueueBytes: logEventBytes(event), GapCount: 1, GapFirstTime: source.Time, GapLastTime: source.Time, Mergeable: true}
	encoded, err := json.Marshal(record)
	if err != nil {
		return "", err
	}
	if err := bucket.Put([]byte(logEventKey(source.BackendID, event.EventID)), encoded); err != nil {
		return "", err
	}
	return event.EventID, nil
}

func validLogEvent(event LogEvent) bool {
	if !bounded(event.BackendID, 128) || !bounded(event.EventID, 128) || !bounded(event.GroupKey, 512) || !bounded(event.UserKey, 512) || event.Time <= 0 || !bounded(event.Kind, 16) || (event.Kind != "message" && event.Kind != "gap") {
		return false
	}
	if len(event.Nickname) > 256 || len(event.Text) > 64*1024 || !utf8.ValidString(event.Nickname) || !utf8.ValidString(event.Text) || strings.ContainsAny(event.BackendID+event.EventID+event.GroupKey+event.UserKey, "\x00\r\n") || strings.ContainsRune(event.Nickname+event.Text, '\x00') {
		return false
	}
	if event.Kind == "message" && event.Text == "" {
		return false
	}
	return true
}

func logEventKey(backendID, eventID string) string { return backendID + "\x00" + eventID }
func logEventBytes(event LogEvent) int {
	return len(event.BackendID) + len(event.EventID) + len(event.GroupKey) + len(event.UserKey) + len(event.Nickname) + len(event.Text) + 128
}
func fixedGapText(count int, first, last int64) string {
	return fmt.Sprintf("[log gap: %d event(s) could not be queued; event times %d through %d]", count, first, last)
}
func fingerprintLogEvent(event LogEvent) string {
	data, _ := json.Marshal(event)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (m *Manager) logWorker(backendID string, b *backend) {
	defer m.workers.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-b.eventWake:
		case <-ticker.C:
		}
		for {
			event, ok, err := m.nextLogEvent(backendID)
			if err != nil || !ok {
				break
			}
			m.mu.Lock()
			call, back := m.pendingCallBeforeLocked(backendID, event.Order), m.backends[backendID]
			ready := back != nil && back.status.Ready
			m.mu.Unlock()
			if call != nil {
				break
			}
			if !ready {
				break
			}
			back.dispatchMu.Lock()
			m.mu.Lock()
			call = m.pendingCallBeforeLocked(backendID, event.Order)
			stillReady := m.backends[backendID] == back && back.status.Ready
			m.mu.Unlock()
			if call != nil || !stillReady {
				back.dispatchMu.Unlock()
				break
			}
			ctx, cancel := context.WithTimeout(m.ctx, captureAckTimeout)
			err = m.sendLogEvent(ctx, backendID, event)
			cancel()
			back.dispatchMu.Unlock()
			if err != nil {
				break
			}
		}
	}
}

func (m *Manager) pendingCallBeforeLocked(backendID string, order uint64) *callState {
	for _, call := range m.records {
		if call.request.BackendID == backendID && call.order != 0 && call.order < order && call.state != "done" {
			return call
		}
	}
	return nil
}

func (m *Manager) nextLogEvent(backendID string) (diskLogEvent, bool, error) {
	var found diskLogEvent
	var ok bool
	err := m.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketLogEvents).ForEach(func(_, value []byte) error {
			var record diskLogEvent
			if err := json.Unmarshal(value, &record); err != nil {
				return err
			}
			if record.Event.BackendID == backendID && (!ok || record.Order < found.Order) {
				found, ok = record, true
			}
			return nil
		})
	})
	return found, ok, err
}

func (m *Manager) dispatchLogEventsBefore(ctx context.Context, backendID string, order uint64) error {
	for {
		event, ok, err := m.nextLogEvent(backendID)
		if err != nil {
			return ErrCaptureStateUnavailable
		}
		if !ok || event.Order >= order {
			return nil
		}
		if event.RetryAt > time.Now().UnixNano() {
			delay := time.Until(time.Unix(0, event.RetryAt))
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-m.stop:
				timer.Stop()
				return context.Canceled
			case <-timer.C:
			}
			continue
		}
		if err := m.sendLogEvent(ctx, backendID, event); err != nil {
			if errors.Is(err, errCaptureAckFailed) || errors.Is(err, errCaptureRetryWait) {
				continue
			}
			return err
		}
	}
}

func (m *Manager) sendLogEvent(ctx context.Context, backendID string, event diskLogEvent) error {
	m.mu.Lock()
	b := m.backends[backendID]
	dispatcher := m.logEventDispatcher
	if b == nil || !b.status.Ready || !hasCapability(b.status.Capabilities, "log-capture-v1") || dispatcher == nil {
		m.mu.Unlock()
		return ErrBackendUnavailable
	}
	connectionID, socketID := b.conn, b.socket
	key := logEventKey(backendID, event.Event.EventID)
	var current diskLogEvent
	err := m.db.Update(func(tx *bolt.Tx) error {
		raw := tx.Bucket(bucketLogEvents).Get([]byte(key))
		if raw == nil {
			return ErrNotFound
		}
		if err := json.Unmarshal(raw, &current); err != nil {
			return err
		}
		if current.InflightConn != "" {
			return ErrConnectionChanged
		}
		if current.RetryAt > time.Now().UnixNano() {
			return errCaptureRetryWait
		}
		current.InflightConn, current.InflightSocket = connectionID, socketID
		current.Attempts++
		encoded, err := json.Marshal(current)
		if err != nil {
			return err
		}
		return tx.Bucket(bucketLogEvents).Put([]byte(key), encoded)
	})
	if err != nil {
		m.mu.Unlock()
		return err
	}
	waiter := make(chan error, 1)
	m.logAckWaiters[key] = waiter
	frame := LogEventFrame{PostType: "_llm_bridge_log_event", Version: 1, ConnectionID: connectionID, EventID: current.Event.EventID, GroupID: current.GroupID, UserID: current.UserID, Time: current.Event.Time, Nickname: current.Event.Nickname, Text: current.Event.Text, IsBot: current.Event.IsBot, Kind: current.Event.Kind}
	message := map[string]interface{}{}
	encodedFrame, _ := json.Marshal(frame)
	_ = json.Unmarshal(encodedFrame, &message)
	m.mu.Unlock()
	if err := dispatcher(ctx, backendID, socketID, connectionID, message); err != nil {
		m.clearLogInflight(backendID, key, connectionID, socketID, waiter, time.Now().Add(5*time.Second).UnixNano())
		return ErrBackendUnavailable
	}
	select {
	case err := <-waiter:
		return err
	case <-ctx.Done():
		m.clearLogInflight(backendID, key, connectionID, socketID, waiter, time.Now().Add(5*time.Second).UnixNano())
		return ctx.Err()
	case <-m.stop:
		m.clearLogInflight(backendID, key, connectionID, socketID, waiter, 0)
		return context.Canceled
	}
}

func (m *Manager) AckLogEvent(backendID, connectionID, eventID, status string) error {
	if !bounded(backendID, 128) || !bounded(connectionID, 128) || !bounded(eventID, 128) || (status != "ok" && status != "failed") {
		return ErrInvalidRequest
	}
	key := logEventKey(backendID, eventID)
	m.mu.Lock()
	b := m.backends[backendID]
	if b == nil || b.conn != connectionID {
		m.mu.Unlock()
		return ErrConnectionChanged
	}
	var waiter chan error
	err := m.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketLogEvents)
		raw := bucket.Get([]byte(key))
		if raw == nil {
			return ErrNotFound
		}
		var record diskLogEvent
		if err := json.Unmarshal(raw, &record); err != nil {
			return err
		}
		if record.InflightConn != connectionID || record.InflightSocket != b.socket {
			return ErrConnectionChanged
		}
		if status == "ok" {
			return bucket.Delete([]byte(key))
		}
		record.InflightConn, record.InflightSocket = "", ""
		record.RetryAt = time.Now().Add(5 * time.Second).UnixNano()
		encoded, err := json.Marshal(record)
		if err != nil {
			return err
		}
		return bucket.Put([]byte(key), encoded)
	})
	if err == nil {
		waiter = m.logAckWaiters[key]
		delete(m.logAckWaiters, key)
	}
	wakeLogWorker(b)
	m.mu.Unlock()
	if err != nil {
		return err
	}
	if waiter != nil {
		if status == "ok" {
			waiter <- nil
		} else {
			waiter <- errCaptureAckFailed
		}
	}
	return nil
}

func (m *Manager) clearLogInflight(backendID, key, connectionID, socketID string, waiter chan error, retryAt int64) {
	m.mu.Lock()
	if m.logAckWaiters[key] != waiter {
		m.mu.Unlock()
		return
	}
	b := m.backends[backendID]
	if b == nil || b.conn != connectionID || b.socket != socketID {
		delete(m.logAckWaiters, key)
		m.mu.Unlock()
		return
	}
	_ = m.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketLogEvents)
		raw := bucket.Get([]byte(key))
		if raw == nil {
			return nil
		}
		var record diskLogEvent
		if err := json.Unmarshal(raw, &record); err != nil {
			return err
		}
		if record.InflightConn == connectionID && record.InflightSocket == socketID {
			record.InflightConn, record.InflightSocket, record.RetryAt = "", "", retryAt
			encoded, err := json.Marshal(record)
			if err != nil {
				return err
			}
			return bucket.Put([]byte(key), encoded)
		}
		return nil
	})
	if m.logAckWaiters[key] == waiter {
		delete(m.logAckWaiters, key)
	}
	if b != nil {
		wakeLogWorker(b)
	}
	m.mu.Unlock()
}

// resetConnectionLogsLocked makes all unacknowledged frames eligible for replay
// on the next registered connection and releases their serialized dispatch waiters.
func (m *Manager) resetConnectionLogsLocked(backendID, connectionID string) error {
	var released []chan error
	err := m.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketLogEvents)
		cursor := bucket.Cursor()
		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			var event diskLogEvent
			if err := json.Unmarshal(value, &event); err != nil {
				return err
			}
			if event.Event.BackendID != backendID || event.InflightConn == "" || (connectionID != "" && event.InflightConn != connectionID) {
				continue
			}
			event.InflightConn, event.InflightSocket, event.RetryAt = "", "", 0
			encoded, err := json.Marshal(event)
			if err != nil {
				return err
			}
			if err := bucket.Put(key, encoded); err != nil {
				return err
			}
			waitKey := logEventKey(backendID, event.Event.EventID)
			if waiter := m.logAckWaiters[waitKey]; waiter != nil {
				released = append(released, waiter)
				delete(m.logAckWaiters, waitKey)
			}
		}
		return nil
	})
	if err != nil {
		return ErrCaptureStateUnavailable
	}
	for _, waiter := range released {
		select {
		case waiter <- ErrConnectionChanged:
		default:
		}
	}
	return nil
}

func wakeLogWorker(b *backend) {
	if b == nil || b.eventWake == nil {
		return
	}
	select {
	case b.eventWake <- struct{}{}:
	default:
	}
}

func (m *Manager) AcceptArtifact(backendID, connectionID string, sourceID int32, filename, mediaType, encodedData string) (ArtifactReceipt, error) {
	if sourceID <= 0 || !validArtifactName(filename, mediaType) || len(encodedData) > ((ArtifactMaxBytes+2)/3)*4+8 {
		return ArtifactReceipt{}, ErrInvalidRequest
	}
	data, err := base64.StdEncoding.Strict().DecodeString(encodedData)
	if err != nil || len(data) == 0 || len(data) > ArtifactMaxBytes || !utf8.Valid(data) || base64.StdEncoding.EncodeToString(data) != encodedData {
		return ArtifactReceipt{}, ErrInvalidRequest
	}
	sum := sha256.Sum256(data)
	metadata := ArtifactReceipt{Filename: filename, MediaType: mediaType, Size: len(data), SHA256: hex.EncodeToString(sum[:])}
	m.mu.Lock()
	defer m.mu.Unlock()
	b, call := m.backends[backendID], m.active[backendID]
	if b == nil || call == nil || b.conn != connectionID || call.connectionID != connectionID || call.requestID != sourceID || call.state != "dispatching" {
		return ArtifactReceipt{}, ErrConnectionChanged
	}
	if !hasCapability(b.status.Capabilities, "artifact-v1") || call.request.Audience != "group" || call.request.GroupKey == "" {
		return ArtifactReceipt{}, ErrCapabilityUnsupported
	}
	if call.acceptedCount >= MaxOutputsPerRequest || len(call.artifacts) >= ArtifactCountLimit {
		return ArtifactReceipt{}, ErrArtifactLimit
	}
	receipt, err := randomID()
	if err != nil {
		return ArtifactReceipt{}, err
	}
	metadata.Receipt = receipt
	key := artifactKey(backendID, call.request.RequestID, receipt)
	err = m.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketArtifacts)
		count, used := 0, 0
		cursor := bucket.Cursor()
		for key, raw := cursor.First(); key != nil; key, raw = cursor.Next() {
			var prior diskArtifact
			if err := json.Unmarshal(raw, &prior); err != nil {
				return err
			}
			if time.Since(time.Unix(0, prior.CreatedAt)) > ArtifactTTL {
				if err := cursor.Delete(); err != nil {
					return err
				}
				continue
			}
			count++
			used += len(prior.Data)
		}
		if count >= ArtifactCountLimit || used+len(data) > ArtifactBytesLimit {
			return ErrArtifactLimit
		}
		requestKey := requestKey(backendID, call.request.RequestID)
		requests := tx.Bucket(bucketRequests)
		raw := requests.Get([]byte(requestKey))
		if raw == nil {
			return ErrNotFound
		}
		var record diskRequest
		if err := json.Unmarshal(raw, &record); err != nil {
			return err
		}
		if record.State != "dispatching" || record.ConnectionID != connectionID || record.SourceID != sourceID || record.AcceptedCount != call.acceptedCount {
			return ErrConnectionChanged
		}
		record.AcceptedCount++
		record.ArtifactReceipts = append(record.ArtifactReceipts, metadata)
		encodedRecord, err := json.Marshal(record)
		if err != nil {
			return err
		}
		if err := requests.Put([]byte(requestKey), encodedRecord); err != nil {
			return err
		}
		artifact := diskArtifact{BackendID: backendID, RequestID: call.request.RequestID, GroupKey: call.request.GroupKey, CreatedAt: time.Now().UnixNano(), Metadata: metadata, Data: append([]byte(nil), data...)}
		encodedArtifact, err := json.Marshal(artifact)
		if err != nil {
			return err
		}
		return bucket.Put([]byte(key), encodedArtifact)
	})
	if err != nil {
		return ArtifactReceipt{}, err
	}
	call.acceptedCount++
	call.artifacts = append(call.artifacts, metadata)
	return metadata, nil
}

func validArtifactName(filename, mediaType string) bool {
	if !bounded(filename, 255) || !utf8.ValidString(filename) || strings.ContainsAny(filename, "/\\:\x00\r\n") || filepath.Base(filename) != filename {
		return false
	}
	ext := strings.ToLower(filepath.Ext(filename))
	if ext == ".md" {
		return mediaType == "text/markdown"
	}
	if ext == ".txt" {
		return mediaType == "text/plain"
	}
	return false
}

func (m *Manager) ClaimArtifact(backendID, requestID, receipt, groupKey string) (ArtifactDelivery, error) {
	if !bounded(backendID, 128) || !bounded(requestID, 128) || !bounded(receipt, 64) || !bounded(groupKey, 512) {
		return ArtifactDelivery{}, ErrInvalidRequest
	}
	var delivery ArtifactDelivery
	var expired bool
	err := m.db.Update(func(tx *bolt.Tx) error {
		requests := tx.Bucket(bucketRequests)
		raw := requests.Get([]byte(requestKey(backendID, requestID)))
		if raw == nil {
			return ErrNotFound
		}
		var request diskRequest
		if err := json.Unmarshal(raw, &request); err != nil {
			return err
		}
		if request.State != "ok" || request.GroupKey != groupKey || time.Since(time.Unix(0, request.CreatedAt)) > RequestTTL {
			return ErrNotFound
		}
		metadataFound := false
		for _, metadata := range request.Result.ArtifactReceipts {
			if metadata.Receipt == receipt {
				metadataFound = true
				break
			}
		}
		if !metadataFound {
			return ErrNotFound
		}
		bucket := tx.Bucket(bucketArtifacts)
		key := artifactKey(backendID, requestID, receipt)
		artifactRaw := bucket.Get([]byte(key))
		if artifactRaw == nil {
			return ErrNotFound
		}
		var artifact diskArtifact
		if err := json.Unmarshal(artifactRaw, &artifact); err != nil {
			return err
		}
		if artifact.GroupKey != groupKey {
			return ErrNotFound
		}
		if time.Since(time.Unix(0, artifact.CreatedAt)) > ArtifactTTL {
			expired = true
			return bucket.Delete([]byte(key))
		}
		if artifact.Claimed {
			return ErrAlreadyClaimed
		}
		deliveryID, err := randomID()
		if err != nil {
			return err
		}
		artifact.Claimed, artifact.DeliveryID = true, deliveryID
		encoded, err := json.Marshal(artifact)
		if err != nil {
			return err
		}
		if err := bucket.Put([]byte(key), encoded); err != nil {
			return err
		}
		delivery = ArtifactDelivery{DeliveryID: deliveryID, ArtifactReceipt: artifact.Metadata, BytesBase64: base64.StdEncoding.EncodeToString(artifact.Data)}
		return nil
	})
	if err == nil && expired {
		return ArtifactDelivery{}, ErrNotFound
	}
	return delivery, err
}

func (m *Manager) AckArtifact(backendID, requestID, receipt, groupKey, deliveryID, status string) error {
	if !bounded(backendID, 128) || !bounded(requestID, 128) || !bounded(receipt, 64) || !bounded(groupKey, 512) || !bounded(deliveryID, 64) || (status != "sent" && status != "failed" && status != "unknown" && status != "expired") {
		return ErrInvalidRequest
	}
	return m.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketArtifacts)
		key := artifactKey(backendID, requestID, receipt)
		raw := bucket.Get([]byte(key))
		if raw == nil {
			return ErrNotFound
		}
		var artifact diskArtifact
		if err := json.Unmarshal(raw, &artifact); err != nil {
			return err
		}
		if artifact.GroupKey != groupKey || !artifact.Claimed || artifact.DeliveryID != deliveryID {
			return ErrNotFound
		}
		return bucket.Delete([]byte(key))
	})
}

func artifactKey(backendID, requestID, receipt string) []byte {
	return []byte(backendID + "\x00" + requestID + "\x00" + receipt)
}

func deleteArtifactsForCall(bucket *bolt.Bucket, backendID, requestID string) error {
	cursor := bucket.Cursor()
	for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
		var artifact diskArtifact
		if err := json.Unmarshal(value, &artifact); err != nil {
			continue
		}
		if artifact.BackendID == backendID && artifact.RequestID == requestID {
			if err := cursor.Delete(); err != nil {
				return err
			}
		}
	}
	return nil
}
