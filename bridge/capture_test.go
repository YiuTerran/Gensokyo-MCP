package bridge

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func captureEvent(id, text string) LogEvent {
	return LogEvent{BackendID: "sealdice", EventID: id, GroupKey: "123:group-1", UserKey: "123:user-1", Time: 1_800_000_000, Nickname: "Member", Text: text, Kind: "message"}
}

func openCaptureTestManager(t *testing.T, path string, capabilities []string) *Manager {
	t.Helper()
	m, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	m.ConfigureBackends([]string{"sealdice"})
	t.Cleanup(func() { _ = m.Close() })
	registerWithCapabilities(t, m, "socket-1", "capture-test", capabilities)
	return m
}

func TestCaptureReplaysOnlyOnNewConnectionAndDeduplicates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bridge.db")
	m, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	m.ConfigureBackends([]string{"sealdice"})
	firstFrames := make(chan map[string]interface{}, 2)
	m.SetLogEventDispatcher(func(_ context.Context, _, _, _ string, frame map[string]interface{}) error {
		firstFrames <- frame
		return nil
	})
	oldConnection := registerWithCapabilities(t, m, "socket-1", "instance-1", []string{"reply", "complete", "log-capture-v1"})
	if err := m.Capture(captureEvent("event-1", "current raw text")); err != nil {
		t.Fatal(err)
	}
	first := receiveFrame(t, firstFrames)
	if first["post_type"] != "_llm_bridge_log_event" || first["event_id"] != "event-1" || first["connection_id"] != oldConnection {
		t.Fatalf("unexpected capture frame: %#v", first)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	m2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()
	m2.ConfigureBackends([]string{"sealdice"})
	secondFrames := make(chan map[string]interface{}, 2)
	m2.SetLogEventDispatcher(func(_ context.Context, _, _, _ string, frame map[string]interface{}) error {
		secondFrames <- frame
		return nil
	})
	newConnection := registerWithCapabilities(t, m2, "socket-2", "instance-2", []string{"reply", "complete", "log-capture-v1"})
	second := receiveFrame(t, secondFrames)
	if second["event_id"] != first["event_id"] || second["connection_id"] != newConnection || second["connection_id"] == oldConnection {
		t.Fatalf("event replay was not bound to the new connection: first=%#v second=%#v", first, second)
	}
	if err := m2.AckLogEvent("sealdice", oldConnection, "event-1", "ok"); !errors.Is(err, ErrConnectionChanged) {
		t.Fatalf("stale connection ACK accepted: %v", err)
	}
	if err := m2.AckLogEvent("sealdice", newConnection, "event-1", "ok"); err != nil {
		t.Fatal(err)
	}
	if err := m2.Capture(captureEvent("event-1", "current raw text")); err != nil {
		t.Fatalf("identical event was not deduplicated: %v", err)
	}
	select {
	case frame := <-secondFrames:
		t.Fatalf("deduplicated event replayed after ACK: %#v", frame)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestLateOldWriterFailureDoesNotClearNewCaptureWaiter(t *testing.T) {
	m := openCaptureTestManager(t, filepath.Join(t.TempDir(), "bridge.db"), []string{"reply", "complete", "log-capture-v1"})
	oldConn, ok := currentConnection(t, m, "sealdice")
	if !ok {
		t.Fatal("registered connection missing")
	}
	oldStarted := make(chan struct{})
	releaseOld := make(chan struct{})
	defer func() {
		select {
		case <-releaseOld:
		default:
			close(releaseOld)
		}
	}()
	newFrames := make(chan map[string]interface{}, 1)
	m.SetLogEventDispatcher(func(_ context.Context, _, _, connectionID string, frame map[string]interface{}) error {
		if connectionID == oldConn {
			close(oldStarted)
			<-releaseOld
			return errors.New("old socket write failed")
		}
		newFrames <- frame
		return nil
	})
	event := captureEvent("late-old-writer", "replay on new connection")
	if err := m.Capture(event); err != nil {
		t.Fatal(err)
	}
	select {
	case <-oldStarted:
	case <-time.After(2 * time.Second):
		close(releaseOld)
		t.Fatal("old connection did not start dispatch")
	}
	newConn := registerWithCapabilities(t, m, "socket-2", "new-process", []string{"reply", "complete", "log-capture-v1"})
	stored, exists, err := m.nextLogEvent("sealdice")
	if err != nil || !exists {
		close(releaseOld)
		t.Fatalf("capture event was not available for replay: exists=%v err=%v", exists, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	newResult := make(chan error, 1)
	go func() { newResult <- m.sendLogEvent(ctx, "sealdice", stored) }()
	newFrame := receiveFrame(t, newFrames)
	if newFrame["connection_id"] != newConn || newFrame["event_id"] != event.EventID {
		close(releaseOld)
		t.Fatalf("capture replay used the wrong connection: %#v", newFrame)
	}
	key := logEventKey(event.BackendID, event.EventID)
	m.mu.Lock()
	newWaiter := m.logAckWaiters[key]
	m.mu.Unlock()
	if newWaiter == nil {
		close(releaseOld)
		t.Fatal("new connection did not install an ACK waiter")
	}
	close(releaseOld)
	// The old worker releases dispatchMu only after its failed write is cleaned up.
	m.backends["sealdice"].dispatchMu.Lock()
	m.backends["sealdice"].dispatchMu.Unlock()
	m.mu.Lock()
	currentWaiter := m.logAckWaiters[key]
	m.mu.Unlock()
	if currentWaiter != newWaiter {
		t.Fatal("old writer cleanup removed the new connection's ACK waiter")
	}
	if err := m.AckLogEvent("sealdice", newConn, event.EventID, "ok"); err != nil {
		t.Fatalf("new connection ACK failed: %v", err)
	}
	select {
	case err := <-newResult:
		if err != nil {
			t.Fatalf("new dispatch did not complete after ACK: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("new dispatch timed out despite its ACK")
	}
}

func TestCaptureQueueAcceptsEventsDuringWebsocketOutage(t *testing.T) {
	m := openCaptureTestManager(t, filepath.Join(t.TempDir(), "bridge.db"), []string{"reply", "complete", "log-capture-v1"})
	frames := make(chan map[string]interface{}, 1)
	m.SetLogEventDispatcher(func(_ context.Context, _, _, _ string, frame map[string]interface{}) error {
		frames <- frame
		return nil
	})
	conn, ok := currentConnection(t, m, "sealdice")
	if !ok {
		t.Fatal("registered connection missing")
	}
	m.Disconnect("sealdice", "socket-1")
	if err := m.Capture(captureEvent("during-outage", "queued raw text")); err != nil {
		t.Fatalf("capture was rejected during websocket outage: %v", err)
	}
	select {
	case frame := <-frames:
		t.Fatalf("event dispatched without a connection: %#v", frame)
	case <-time.After(100 * time.Millisecond):
	}
	newConn := registerWithCapabilities(t, m, "socket-2", "capture-test-2", []string{"reply", "complete", "log-capture-v1"})
	frame := receiveFrame(t, frames)
	if frame["event_id"] != "during-outage" || frame["connection_id"] != newConn || frame["connection_id"] == conn {
		t.Fatalf("outage event was not queued and replayed on new connection: %#v", frame)
	}
	if err := m.AckLogEvent("sealdice", newConn, "during-outage", "ok"); err != nil {
		t.Fatal(err)
	}
}

func TestCaptureSeenFingerprintSurvivesTTLWhileEventIsPending(t *testing.T) {
	m := openCaptureTestManager(t, filepath.Join(t.TempDir(), "bridge.db"), []string{"reply", "complete", "log-capture-v1"})
	event := captureEvent("pending-seen-expiry", "original body")
	if err := m.Capture(event); err != nil {
		t.Fatal(err)
	}
	if err := m.db.Update(func(tx *bolt.Tx) error {
		key := []byte(logEventKey(event.BackendID, event.EventID))
		var seen diskLogSeen
		if err := json.Unmarshal(tx.Bucket(bucketLogSeen).Get(key), &seen); err != nil {
			return err
		}
		seen.CreatedAt = time.Now().Add(-RequestTTL - time.Second).UnixNano()
		encoded, err := json.Marshal(seen)
		if err != nil {
			return err
		}
		return tx.Bucket(bucketLogSeen).Put(key, encoded)
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.prune(time.Now()); err != nil {
		t.Fatal(err)
	}
	conflict := event
	conflict.Text = "different body"
	if err := m.Capture(conflict); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("pending event fingerprint expired and allowed ID reuse: %v", err)
	}
}

func TestCommandWaitsForFailedCaptureRetryBeforeDispatch(t *testing.T) {
	m := openCaptureTestManager(t, filepath.Join(t.TempDir(), "bridge.db"), []string{"reply", "complete", "log-capture-v1"})
	frames := make(chan map[string]interface{}, 4)
	m.SetLogEventDispatcher(func(_ context.Context, _, _, _ string, frame map[string]interface{}) error {
		frames <- frame
		return nil
	})
	event := captureEvent("retry-before-command", "ordered")
	if err := m.Capture(event); err != nil {
		t.Fatal(err)
	}
	first := receiveFrame(t, frames)
	conn, ok := currentConnection(t, m, "sealdice")
	if !ok {
		t.Fatal("registered connection missing")
	}
	if err := m.AckLogEvent("sealdice", conn, event.EventID, "failed"); err != nil {
		t.Fatal(err)
	}
	canceledCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	canceledDispatches := 0
	canceledRequest := groupRequest("cancel-during-capture-retry")
	canceledResult, canceledErr := m.Call(canceledCtx, canceledRequest, func(context.Context, string, string, int32) error {
		canceledDispatches++
		return nil
	})
	cancel()
	if !errors.Is(canceledErr, context.DeadlineExceeded) || canceledDispatches != 0 {
		t.Fatalf("canceled queued command was replayed across capture retry: result=%+v err=%v dispatches=%d", canceledResult, canceledErr, canceledDispatches)
	}
	deadline := time.Now().Add(time.Second)
	canceledDone := false
	for time.Now().Before(deadline) {
		m.mu.Lock()
		call := m.records[requestKey(canceledRequest.BackendID, canceledRequest.RequestID)]
		done := call != nil && call.state == "done"
		quarantined := m.backends["sealdice"].quarantine != nil
		m.mu.Unlock()
		if done {
			canceledDone = true
			if quarantined {
				t.Fatal("command canceled before dispatch quarantined the backend")
			}
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !canceledDone {
		t.Fatal("canceled command did not become terminal while waiting for capture retry")
	}
	if err := m.db.Update(func(tx *bolt.Tx) error {
		key := []byte(logEventKey("sealdice", event.EventID))
		var record diskLogEvent
		if err := json.Unmarshal(tx.Bucket(bucketLogEvents).Get(key), &record); err != nil {
			return err
		}
		record.RetryAt = time.Now().Add(300 * time.Millisecond).UnixNano()
		encoded, err := json.Marshal(record)
		if err != nil {
			return err
		}
		return tx.Bucket(bucketLogEvents).Put(key, encoded)
	}); err != nil {
		t.Fatal(err)
	}

	dispatched := make(chan struct{}, 1)
	resultCh := make(chan testCallResult, 1)
	request := groupRequest("wait-for-capture-retry")
	go func() {
		result, err := m.Call(context.Background(), request, func(_ context.Context, connectionID, _ string, id int32) error {
			dispatched <- struct{}{}
			return m.Complete("sealdice", connectionID, id, "ok", 0)
		})
		resultCh <- testCallResult{result: result, err: err}
	}()
	select {
	case <-dispatched:
		t.Fatal("command crossed the failed capture retry delay")
	case <-time.After(100 * time.Millisecond):
	}
	second := receiveFrame(t, frames)
	if second["event_id"] != first["event_id"] || second["connection_id"] != conn {
		t.Fatalf("retry did not replay the same event on the current connection: first=%#v second=%#v", first, second)
	}
	if err := m.AckLogEvent("sealdice", conn, event.EventID, "ok"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-dispatched:
	case <-time.After(2 * time.Second):
		t.Fatal("command did not dispatch after retried capture ACK")
	}
	if result := <-resultCh; result.err != nil || result.result.Status != "ok" {
		t.Fatalf("command result after retry: %+v", result)
	}
}

func TestOverflowGapCoalescesOnlyAtGroupDispatchTail(t *testing.T) {
	m := openCaptureTestManager(t, filepath.Join(t.TempDir(), "bridge.db"), []string{"reply", "complete", "log-capture-v1"})
	const seedCount = CaptureEventLimit
	groupKey := "123:tail-group"
	if err := m.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketLogEvents)
		for i := 0; i < seedCount; i++ {
			event := LogEvent{BackendID: "sealdice", EventID: fmt.Sprintf("tail-seed-%d", i), GroupKey: groupKey, UserKey: "123:user", Time: 1_800_000_000, Kind: "message", Text: "seed"}
			record := diskLogEvent{Event: event, Order: uint64(i + 1), QueueBytes: 1}
			encoded, err := json.Marshal(record)
			if err != nil {
				return err
			}
			if err := bucket.Put([]byte(logEventKey(event.BackendID, event.EventID)), encoded); err != nil {
				return err
			}
		}
		meta := tx.Bucket(bucketMeta)
		return meta.Put(dispatchOrderKey("sealdice"), sequenceBytes(seedCount))
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.Capture(captureEventForGroup("tail-drop-1", groupKey)); !errors.Is(err, ErrCaptureQueueFull) {
		t.Fatalf("first overflow result: %v", err)
	}
	commandOrder, err := m.reserveDispatchOrder("sealdice")
	if err != nil {
		t.Fatal(err)
	}
	commandRecord := diskRequest{State: "done", CreatedAt: time.Now().UnixNano(), GroupKey: groupKey, Order: commandOrder, Result: Result{BackendID: "sealdice", RequestID: "tail-command", Audience: "group", Status: "ok", Outputs: []Output{}}}
	if err := m.writeRequest(requestKey("sealdice", "tail-command"), commandRecord); err != nil {
		t.Fatal(err)
	}
	if err := m.Capture(captureEventForGroup("tail-drop-after-command", groupKey)); !errors.Is(err, ErrCaptureQueueFull) {
		t.Fatalf("overflow after command result: %v", err)
	}
	if err := m.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketLogEvents).Delete([]byte(logEventKey("sealdice", "tail-seed-0")))
	}); err != nil {
		t.Fatal(err)
	}
	between := captureEventForGroup("tail-accepted-between", groupKey)
	if err := m.Capture(between); err != nil {
		t.Fatalf("accepted event between losses: %v", err)
	}
	if err := m.Capture(captureEventForGroup("tail-drop-3", groupKey)); !errors.Is(err, ErrCaptureQueueFull) {
		t.Fatalf("third overflow result: %v", err)
	}
	var gapOrders []uint64
	var betweenOrder uint64
	if err := m.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketLogEvents).ForEach(func(_, raw []byte) error {
			var record diskLogEvent
			if err := json.Unmarshal(raw, &record); err != nil {
				return err
			}
			if record.Event.GroupKey == groupKey && record.GapCount > 0 {
				gapOrders = append(gapOrders, record.Order)
				if record.GapCount != 1 {
					t.Errorf("loss after an accepted event was merged into an earlier gap: %+v", record)
				}
			}
			if record.Event.EventID == between.EventID {
				betweenOrder = record.Order
			}
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
	sort.Slice(gapOrders, func(i, j int) bool { return gapOrders[i] < gapOrders[j] })
	if len(gapOrders) != 3 || betweenOrder == 0 || !(gapOrders[0] < commandOrder && commandOrder < gapOrders[1] && gapOrders[1] < betweenOrder && betweenOrder < gapOrders[2]) {
		t.Fatalf("gap markers violated group event/command dispatch order: gaps=%v command=%d between=%d", gapOrders, commandOrder, betweenOrder)
	}
}

func captureEventForGroup(id, group string) LogEvent {
	event := captureEvent(id, "text")
	event.GroupKey = group
	return event
}

func TestCaptureHTTPDurableAcceptanceAndQueueFullGap(t *testing.T) {
	m := openCaptureTestManager(t, filepath.Join(t.TempDir(), "bridge.db"), []string{"reply", "complete", "log-capture-v1"})
	handler := InternalHandler(m, "internal-token")
	post := func(event LogEvent) *httptest.ResponseRecorder {
		t.Helper()
		body, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/internal/log/events", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer internal-token")
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, req)
		return recorder
	}
	accepted := post(captureEvent("http-accepted", "hello"))
	var acceptedBody map[string]any
	if err := json.Unmarshal(accepted.Body.Bytes(), &acceptedBody); err != nil {
		t.Fatal(err)
	}
	if accepted.Code != http.StatusAccepted || acceptedBody["accepted"] != true {
		t.Fatalf("event was not durably accepted: status=%d body=%s", accepted.Code, accepted.Body.String())
	}

	if err := m.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketLogEvents)
		for i := 0; i < CaptureEventLimit; i++ {
			event := diskLogEvent{Event: LogEvent{BackendID: "sealdice", EventID: fmt.Sprintf("seed-%d", i), GroupKey: "123:occupied", UserKey: "123:member", Time: 1_800_000_000, Kind: "message", Text: "seed"}, Order: uint64(i + 1), QueueBytes: 1}
			encoded, err := json.Marshal(event)
			if err != nil {
				return err
			}
			if err := bucket.Put([]byte(logEventKey("sealdice", event.Event.EventID)), encoded); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	dropped := captureEvent("overflow", "DO NOT PERSIST THIS RAW BODY")
	response := post(dropped)
	var errorBody map[string]string
	if err := json.Unmarshal(response.Body.Bytes(), &errorBody); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusTooManyRequests || errorBody["error"] != "queue_full" {
		t.Fatalf("unexpected full-queue response: status=%d body=%s", response.Code, response.Body.String())
	}
	duplicate := post(dropped)
	if duplicate.Code != http.StatusTooManyRequests || !strings.Contains(duplicate.Body.String(), `"error":"queue_full"`) {
		t.Fatalf("duplicate rejected event did not receive fixed queue_full response: status=%d body=%s", duplicate.Code, duplicate.Body.String())
	}
	conflicting := post(LogEvent{BackendID: dropped.BackendID, EventID: dropped.EventID, GroupKey: dropped.GroupKey, UserKey: dropped.UserKey, Time: dropped.Time, Text: "different body", Kind: "message"})
	if conflicting.Code != http.StatusConflict || !strings.Contains(conflicting.Body.String(), `"error":"request_conflict"`) {
		t.Fatalf("rejected event ID reuse with different content was not rejected: status=%d body=%s", conflicting.Code, conflicting.Body.String())
	}
	if err := m.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketLogEvents).Delete([]byte(logEventKey("sealdice", "seed-0")))
	}); err != nil {
		t.Fatal(err)
	}
	afterRecovery := post(dropped)
	if afterRecovery.Code != http.StatusTooManyRequests || !strings.Contains(afterRecovery.Body.String(), `"error":"queue_full"`) {
		t.Fatalf("previously dropped ID was accepted after capacity recovered: status=%d body=%s", afterRecovery.Code, afterRecovery.Body.String())
	}
	if err := m.db.View(func(tx *bolt.Tx) error {
		var found bool
		gapCount := 0
		if err := tx.Bucket(bucketLogEvents).ForEach(func(_, raw []byte) error {
			var record diskLogEvent
			if err := json.Unmarshal(raw, &record); err != nil {
				return err
			}
			if record.Event.EventID == "overflow" {
				t.Fatal("rejected raw event was persisted")
			}
			if record.GapCount > 0 && record.Event.GroupKey == dropped.GroupKey {
				found = true
				gapCount += record.GapCount
				if record.Event.Kind != "gap" || strings.Contains(record.Event.Text, dropped.Text) {
					t.Fatalf("gap contains rejected content: %+v", record.Event)
				}
			}
			return nil
		}); err != nil {
			return err
		}
		if !found || gapCount != 1 {
			t.Fatalf("queue overflow gap was missing or counted duplicate retries: found=%v count=%d", found, gapCount)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCaptureBeforeCommandSerializesThroughAck(t *testing.T) {
	m := openCaptureTestManager(t, filepath.Join(t.TempDir(), "bridge.db"), []string{"reply", "complete", "log-capture-v1"})
	frames := make(chan map[string]interface{}, 2)
	m.SetLogEventDispatcher(func(_ context.Context, _, _, _ string, frame map[string]interface{}) error {
		frames <- frame
		return nil
	})
	conn := ""
	for _, status := range m.Backends() {
		_ = status
	}
	conn, ok := currentConnection(t, m, "sealdice")
	if !ok {
		t.Fatal("registered connection missing")
	}
	if err := m.Capture(captureEvent("before-command", "ordered")); err != nil {
		t.Fatal(err)
	}
	_ = receiveFrame(t, frames)
	dispatched := make(chan struct{}, 1)
	result := make(chan Result, 1)
	go func() {
		r, _ := m.Call(context.Background(), groupRequest("ordered-command"), func(_ context.Context, gotConn, _ string, id int32) error {
			dispatched <- struct{}{}
			return m.Complete("sealdice", gotConn, id, "ok", 0)
		})
		result <- r
	}()
	select {
	case <-dispatched:
		t.Fatal("command crossed an unacknowledged capture event")
	case <-time.After(100 * time.Millisecond):
	}
	if err := m.AckLogEvent("sealdice", conn, "before-command", "ok"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-dispatched:
	case <-time.After(2 * time.Second):
		t.Fatal("command did not proceed after capture ACK")
	}
	if r := <-result; r.Status != "ok" {
		t.Fatalf("command result after capture ACK: %+v", r)
	}
}

func TestArtifactClaimBindsGroupAndReturnsOnlyUTF8Bytes(t *testing.T) {
	m := openCaptureTestManager(t, filepath.Join(t.TempDir(), "bridge.db"), []string{"reply", "complete", "artifact-v1"})
	userID, err := m.MapIdentity("sealdice", "user", "123:user")
	if err != nil {
		t.Fatal(err)
	}
	groupID, err := m.MapIdentity("sealdice", "group", "123:group")
	if err != nil {
		t.Fatal(err)
	}
	request := Request{BackendID: "sealdice", RequestID: "artifact-call", Audience: "group", Payload: ".report", UserKey: "123:user", GroupKey: "123:group", UserID: userID, GroupID: groupID}
	content := []byte("# Report\nUTF-8: 世界\n")
	encoded := base64.StdEncoding.EncodeToString(content)
	var sourceID int32
	result, err := m.Call(context.Background(), request, func(_ context.Context, connectionID, _ string, id int32) error {
		sourceID = id
		if _, err := m.AcceptArtifact("sealdice", connectionID, id, "report.md", "text/markdown", encoded); err != nil {
			return err
		}
		return m.Complete("sealdice", connectionID, id, "ok", 1)
	})
	if err != nil || result.Status != "ok" || len(result.ArtifactReceipts) != 1 {
		t.Fatalf("artifact call did not complete with receipt: result=%+v err=%v", result, err)
	}
	serialized, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(serialized, content) || bytes.Contains(serialized, []byte("bytes_base64")) {
		t.Fatalf("model result contains artifact body: %s", serialized)
	}
	receipt := result.ArtifactReceipts[0]
	handler := InternalHandler(m, "internal-token")
	claim := func(groupKey string) *httptest.ResponseRecorder {
		t.Helper()
		body, err := json.Marshal(map[string]string{"backend_id": "sealdice", "request_id": request.RequestID, "receipt": receipt.Receipt, "group_key": groupKey})
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/internal/artifacts/claim", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer internal-token")
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, req)
		return recorder
	}
	if wrong := claim("123:wrong-group"); wrong.Code != http.StatusNotFound {
		t.Fatalf("cross-group claim status=%d body=%s", wrong.Code, wrong.Body.String())
	}
	claimedResponse := claim(request.GroupKey)
	if claimedResponse.Code != http.StatusOK {
		t.Fatalf("claim failed: status=%d body=%s", claimedResponse.Code, claimedResponse.Body.String())
	}
	var delivery ArtifactDelivery
	if err := json.Unmarshal(claimedResponse.Body.Bytes(), &delivery); err != nil {
		t.Fatal(err)
	}
	claimed, err := base64.StdEncoding.DecodeString(delivery.BytesBase64)
	if err != nil || !bytes.Equal(claimed, content) || delivery.Size != len(content) || delivery.SHA256 != receipt.SHA256 {
		t.Fatalf("claimed bytes or metadata mismatch: delivery=%+v err=%v", delivery, err)
	}
	if duplicate := claim(request.GroupKey); duplicate.Code != http.StatusConflict {
		t.Fatalf("second claim status=%d body=%s", duplicate.Code, duplicate.Body.String())
	}
	ackBody, err := json.Marshal(map[string]string{"backend_id": "sealdice", "request_id": request.RequestID, "receipt": receipt.Receipt, "group_key": request.GroupKey, "delivery_id": delivery.DeliveryID, "status": "sent"})
	if err != nil {
		t.Fatal(err)
	}
	ackRequest := httptest.NewRequest(http.MethodPost, "/internal/artifacts/ack", bytes.NewReader(ackBody))
	ackRequest.Header.Set("Authorization", "Bearer internal-token")
	ackResponse := httptest.NewRecorder()
	handler.ServeHTTP(ackResponse, ackRequest)
	if ackResponse.Code != http.StatusOK {
		t.Fatalf("artifact ACK failed: status=%d body=%s", ackResponse.Code, ackResponse.Body.String())
	}
	if gone := claim(request.GroupKey); gone.Code != http.StatusNotFound {
		t.Fatalf("final ACK left artifact claimable: status=%d", gone.Code)
	}
	if err := m.Complete("sealdice", "wrong-connection", sourceID, "ok", 1); !errors.Is(err, ErrConnectionChanged) {
		t.Fatalf("stale completion accepted after result: %v", err)
	}
}

func TestCommandCompletionPrecedesLaterCaptureDispatch(t *testing.T) {
	m := openCaptureTestManager(t, filepath.Join(t.TempDir(), "bridge.db"), []string{"reply", "complete", "log-capture-v1"})
	frames := make(chan map[string]interface{}, 2)
	m.SetLogEventDispatcher(func(_ context.Context, _, _, _ string, frame map[string]interface{}) error {
		frames <- frame
		return nil
	})
	commandStarted := make(chan int32, 1)
	releaseCommand := make(chan struct{})
	result := make(chan Result, 1)
	go func() {
		r, _ := m.Call(context.Background(), groupRequest("before-capture"), func(_ context.Context, connectionID, _ string, id int32) error {
			commandStarted <- id
			<-releaseCommand
			return m.Complete("sealdice", connectionID, id, "ok", 0)
		})
		result <- r
	}()
	commandID := waitMessageID(t, commandStarted)
	if err := m.Capture(captureEvent("after-command", "ordered")); err != nil {
		t.Fatal(err)
	}
	select {
	case frame := <-frames:
		t.Fatalf("capture event crossed an active command: %#v", frame)
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseCommand)
	if r := <-result; r.Status != "ok" {
		t.Fatalf("command result: %+v", r)
	}
	frame := receiveFrame(t, frames)
	if frame["event_id"] != "after-command" {
		t.Fatalf("wrong capture event after command %d: %#v", commandID, frame)
	}
	connectionID, ok := currentConnection(t, m, "sealdice")
	if !ok {
		t.Fatal("registered connection missing")
	}
	if err := m.AckLogEvent("sealdice", connectionID, "after-command", "ok"); err != nil {
		t.Fatal(err)
	}
}

func TestArtifactLimitsAndExpirationAreFinal(t *testing.T) {
	m := openCaptureTestManager(t, filepath.Join(t.TempDir(), "bridge.db"), []string{"reply", "complete", "artifact-v1"})
	tooLarge := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("a", ArtifactMaxBytes+1)))
	if _, err := m.AcceptArtifact("sealdice", "no-connection", 1, "large.txt", "text/plain", tooLarge); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("over-10-MiB artifact was not rejected: %v", err)
	}

	userID, _ := m.MapIdentity("sealdice", "user", "123:user")
	groupID, _ := m.MapIdentity("sealdice", "group", "123:group")
	request := Request{BackendID: "sealdice", RequestID: "expired-artifact", Audience: "group", Payload: ".report", UserKey: "123:user", GroupKey: "123:group", UserID: userID, GroupID: groupID}
	var receipt ArtifactReceipt
	result, err := m.Call(context.Background(), request, func(_ context.Context, connectionID, _ string, id int32) error {
		var acceptErr error
		receipt, acceptErr = m.AcceptArtifact("sealdice", connectionID, id, "report.txt", "text/plain", base64.StdEncoding.EncodeToString([]byte("x")))
		if acceptErr != nil {
			return acceptErr
		}
		return m.Complete("sealdice", connectionID, id, "ok", 1)
	})
	if err != nil || len(result.ArtifactReceipts) != 1 {
		t.Fatalf("artifact setup failed: result=%+v err=%v", result, err)
	}
	if err := m.db.Update(func(tx *bolt.Tx) error {
		key := artifactKey(request.BackendID, request.RequestID, receipt.Receipt)
		var artifact diskArtifact
		raw := tx.Bucket(bucketArtifacts).Get(key)
		if err := json.Unmarshal(raw, &artifact); err != nil {
			return err
		}
		artifact.CreatedAt = time.Now().Add(-ArtifactTTL - time.Second).UnixNano()
		encoded, err := json.Marshal(artifact)
		if err != nil {
			return err
		}
		return tx.Bucket(bucketArtifacts).Put(key, encoded)
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.prune(time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := m.ClaimArtifact("sealdice", request.RequestID, receipt.Receipt, request.GroupKey); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expired artifact was claimable: %v", err)
	}

	limitRequest := request
	limitRequest.RequestID = "artifact-capacity"
	var actionErr error
	limitResult, err := m.Call(context.Background(), limitRequest, func(_ context.Context, connectionID, _ string, id int32) error {
		if err := m.db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(bucketArtifacts)
			for i := 0; i < ArtifactCountLimit; i++ {
				artifact := diskArtifact{BackendID: "other", RequestID: fmt.Sprintf("seed-%d", i), GroupKey: "g", CreatedAt: time.Now().UnixNano(), Data: []byte("x")}
				encoded, err := json.Marshal(artifact)
				if err != nil {
					return err
				}
				if err := bucket.Put(artifactKey("other", artifact.RequestID, fmt.Sprintf("r-%d", i)), encoded); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		_, actionErr = m.AcceptArtifact("sealdice", connectionID, id, "blocked.txt", "text/plain", base64.StdEncoding.EncodeToString([]byte("blocked")))
		return m.Complete("sealdice", connectionID, id, "failed", 0)
	})
	if err != nil || !errors.Is(actionErr, ErrArtifactLimit) || limitResult.Status != "failed" || len(limitResult.ArtifactReceipts) != 0 {
		t.Fatalf("artifact capacity rejection changed completion count: result=%+v callErr=%v actionErr=%v", limitResult, err, actionErr)
	}
}

func TestArtifactAndMessageOutputsShareCompletionCount(t *testing.T) {
	m := openCaptureTestManager(t, filepath.Join(t.TempDir(), "bridge.db"), []string{"reply", "complete", "artifact-v1"})
	userID, err := m.MapIdentity("sealdice", "user", "123:count-user")
	if err != nil {
		t.Fatal(err)
	}
	groupID, err := m.MapIdentity("sealdice", "group", "123:count-group")
	if err != nil {
		t.Fatal(err)
	}
	request := Request{BackendID: "sealdice", RequestID: "mixed-output-count", Audience: "group", Payload: ".report", UserKey: "123:count-user", GroupKey: "123:count-group", UserID: userID, GroupID: groupID}
	var extraErr error
	result, err := m.Call(context.Background(), request, func(_ context.Context, connectionID, _ string, id int32) error {
		for i := 0; i < ArtifactCountLimit; i++ {
			name := fmt.Sprintf("report-%02d.txt", i)
			if _, err := m.AcceptArtifact("sealdice", connectionID, id, name, "text/plain", base64.StdEncoding.EncodeToString([]byte("x"))); err != nil {
				return err
			}
		}
		for i := ArtifactCountLimit; i < MaxOutputsPerRequest; i++ {
			if err := m.Deliver("sealdice", connectionID, id, "send_group_msg", "group", groupID, "ok"); err != nil {
				return err
			}
		}
		extraErr = m.Deliver("sealdice", connectionID, id, "send_group_msg", "group", groupID, "over limit")
		return m.Complete("sealdice", connectionID, id, "ok", MaxOutputsPerRequest)
	})
	if err != nil || result.Status != "ok" || len(result.ArtifactReceipts) != ArtifactCountLimit || len(result.Outputs) != MaxOutputsPerRequest-ArtifactCountLimit {
		t.Fatalf("mixed output completion mismatch: result=%+v err=%v", result, err)
	}
	if !errors.Is(extraErr, ErrInvalidRequest) {
		t.Fatalf("text output beyond combined artifact/text limit accepted: %v", extraErr)
	}
}

func TestPendingCaptureOnLegacyReconnectDoesNotBlockOrdinaryCommands(t *testing.T) {
	m, err := Open(filepath.Join(t.TempDir(), "bridge.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	m.ConfigureBackends([]string{"sealdice"})
	frames := make(chan map[string]interface{}, 2)
	m.SetLogEventDispatcher(func(_ context.Context, _, _, _ string, frame map[string]interface{}) error {
		frames <- frame
		return nil
	})
	_ = registerWithCapabilities(t, m, "capture-socket", "capture-instance", []string{"reply", "complete", "log-capture-v1"})
	if err := m.Capture(captureEvent("pending-on-downgrade", "waiting for ACK")); err != nil {
		t.Fatal(err)
	}
	_ = receiveFrame(t, frames) // leave the old connection's event unacknowledged
	_ = registerWithCapabilities(t, m, "legacy-socket", "legacy-instance", []string{"reply", "complete"})

	ordinary := groupRequest("ordinary-with-pending-capture")
	result, err := m.Call(context.Background(), ordinary, func(_ context.Context, connectionID, _ string, id int32) error {
		return m.Complete("sealdice", connectionID, id, "ok", 0)
	})
	if err != nil || result.Status != "ok" {
		t.Fatalf("ordinary command was blocked by old capture backlog: result=%+v err=%v", result, err)
	}

	logRequest := groupRequest("log-without-capture-capability")
	logRequest.Payload = ".log on"
	dispatches := 0
	result, err = m.Call(context.Background(), logRequest, func(context.Context, string, string, int32) error {
		dispatches++
		return nil
	})
	if !errors.Is(err, ErrCapabilityUnsupported) || result.Status != "failed" || dispatches != 0 {
		t.Fatalf(".log command was not rejected before dispatch: result=%+v err=%v dispatches=%d", result, err, dispatches)
	}

	newConn := registerWithCapabilities(t, m, "capture-restored-socket", "capture-restored-instance", []string{"reply", "complete", "log-capture-v1"})
	replayed := receiveFrame(t, frames)
	if replayed["event_id"] != "pending-on-downgrade" || replayed["connection_id"] != newConn {
		t.Fatalf("pending event was lost or replayed on wrong connection: %#v", replayed)
	}
	if err := m.AckLogEvent("sealdice", newConn, "pending-on-downgrade", "ok"); err != nil {
		t.Fatal(err)
	}
}

func TestCaptureRejectionBeforeCommandDoesNotReplayOrQuarantine(t *testing.T) {
	m := openCaptureTestManager(t, filepath.Join(t.TempDir(), "bridge.db"), []string{"reply", "complete", "log-capture-v1"})
	attempted := make(chan struct{}, 1)
	m.SetLogEventDispatcher(func(context.Context, string, string, string, map[string]interface{}) error {
		attempted <- struct{}{}
		return errors.New("write failure")
	})
	if err := m.Capture(captureEvent("blocked-event", "ordered")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-attempted:
	case <-time.After(2 * time.Second):
		t.Fatal("capture event was not dispatched")
	}
	if err := m.db.Update(func(tx *bolt.Tx) error {
		key := []byte(logEventKey("sealdice", "blocked-event"))
		var record diskLogEvent
		if err := json.Unmarshal(tx.Bucket(bucketLogEvents).Get(key), &record); err != nil {
			return err
		}
		record.RetryAt = time.Now().Add(100 * time.Millisecond).UnixNano()
		encoded, err := json.Marshal(record)
		if err != nil {
			return err
		}
		return tx.Bucket(bucketLogEvents).Put(key, encoded)
	}); err != nil {
		t.Fatal(err)
	}
	var dispatches int
	r, err := m.Call(context.Background(), groupRequest("after-blocked-event"), func(context.Context, string, string, int32) error { dispatches++; return nil })
	if err == nil || r.Status != "unknown" || dispatches != 0 {
		t.Fatalf("command crossed pending capture event or was replayed: result=%+v err=%v dispatches=%d", r, err, dispatches)
	}
	m.mu.Lock()
	quarantined := m.backends["sealdice"].quarantine != nil
	m.mu.Unlock()
	if quarantined {
		t.Fatal("a command rejected before dispatch quarantined the backend")
	}
}

func receiveFrame(t *testing.T, frames <-chan map[string]interface{}) map[string]interface{} {
	t.Helper()
	select {
	case frame := <-frames:
		return frame
	case <-time.After(2 * time.Second):
		t.Fatal("capture frame was not dispatched")
		return nil
	}
}

func currentConnection(t *testing.T, m *Manager, backendID string) (string, bool) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	b := m.backends[backendID]
	if b == nil {
		return "", false
	}
	return b.conn, b.conn != ""
}
