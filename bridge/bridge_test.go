package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func openManager(t *testing.T, path string) *Manager {
	t.Helper()
	m, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	m.ConfigureBackends([]string{"sealdice"})
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Errorf("close manager: %v", err)
		}
	})
	return m
}

func register(t *testing.T, m *Manager, socket string) string {
	t.Helper()
	conn, err := m.Register("sealdice", socket, RegisterParams{Version: 1, BackendInstance: "nonebot-test", Capabilities: []string{"reply", "complete"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Activate("sealdice", socket, conn); err != nil {
		t.Fatal(err)
	}
	return conn
}

func groupRequest(id string) Request {
	return Request{BackendID: "sealdice", RequestID: id, Audience: "group", Payload: ".test", UserID: 11001, GroupID: 22001}
}

func waitMessageID(t *testing.T, ch <-chan int32) int32 {
	t.Helper()
	select {
	case id := <-ch:
		return id
	case <-time.After(2 * time.Second):
		t.Fatal("request was not dispatched")
		return 0
	}
}

func TestDelayedSplitCompletionAndNextRequestIsolation(t *testing.T) {
	m := openManager(t, filepath.Join(t.TempDir(), "bridge.db"))
	conn := register(t, m, "socket-1")
	firstID := make(chan int32, 1)
	firstDone := make(chan Result, 1)
	go func() {
		r, _ := m.Call(context.Background(), groupRequest("split"), func(_ context.Context, gotConn, gotSocket string, id int32) error {
			if gotConn != conn || gotSocket != "socket-1" {
				return ErrConnectionChanged
			}
			firstID <- id
			return nil
		})
		firstDone <- r
	}()
	id1 := waitMessageID(t, firstID)
	if err := m.Deliver("sealdice", conn, id1, "send_group_msg", "group", 22001, "first output"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(40 * time.Millisecond)
	if err := m.Deliver("sealdice", conn, id1, "send_group_msg", "group", 22001, "second delayed output"); err != nil {
		t.Fatal(err)
	}
	if err := m.Complete("sealdice", conn, id1, "ok", 2); err != nil {
		t.Fatal(err)
	}
	result := <-firstDone
	if result.Status != "ok" || len(result.Outputs) != 2 || result.Outputs[1].Message != "second delayed output" {
		t.Fatalf("split result: %+v", result)
	}

	secondID := make(chan int32, 1)
	secondDone := make(chan Result, 1)
	go func() {
		r, _ := m.Call(context.Background(), groupRequest("next"), func(_ context.Context, _, _ string, id int32) error { secondID <- id; return nil })
		secondDone <- r
	}()
	id2 := waitMessageID(t, secondID)
	if id2 == id1 {
		t.Fatal("source message ID was reused")
	}
	if err := m.Deliver("sealdice", conn, id1, "send_group_msg", "group", 22001, "late stale output"); !errors.Is(err, ErrConnectionChanged) {
		t.Fatalf("stale output accepted: %v", err)
	}
	if err := m.Deliver("sealdice", conn, id2, "send_group_msg", "group", 22001, "second request"); err != nil {
		t.Fatal(err)
	}
	if err := m.Complete("sealdice", conn, id2, "ok", 1); err != nil {
		t.Fatal(err)
	}
	if got := (<-secondDone).Outputs; len(got) != 1 || got[0].Message != "second request" {
		t.Fatalf("second result: %+v", got)
	}
}

func TestPrivateOutboxIsAtomicAndGroupResultHasNoPrivateBody(t *testing.T) {
	m := openManager(t, filepath.Join(t.TempDir(), "bridge.db"))
	conn := register(t, m, "socket-1")
	ids := make(chan int32, 1)
	done := make(chan Result, 1)
	go func() {
		r, _ := m.Call(context.Background(), groupRequest("private"), func(_ context.Context, _, _ string, id int32) error { ids <- id; return nil })
		done <- r
	}()
	id := waitMessageID(t, ids)
	if err := m.Deliver("sealdice", conn, id, "send_private_msg", "private", 11001, "secret fixture"); err != nil {
		t.Fatal(err)
	}
	if err := m.Deliver("sealdice", conn, id, "send_group_msg", "group", 22001, "public fixture"); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	receipt := m.active["sealdice"].receipt
	m.mu.Unlock()
	if _, err := m.ClaimPrivate("sealdice", "private", receipt); !errors.Is(err, ErrNotFound) {
		t.Fatalf("pending request outbox was claimable: %v", err)
	}
	if err := m.Complete("sealdice", conn, id, "ok", 2); err != nil {
		t.Fatal(err)
	}
	r := <-done
	if len(r.Outputs) != 1 || r.Outputs[0].Message != "public fixture" || r.PrivateCount != 1 || r.PrivateReceipt == "" {
		t.Fatalf("group result leaked/omitted content: %+v", r)
	}
	delivery, err := m.ClaimPrivate("sealdice", "private", r.PrivateReceipt)
	if err != nil {
		t.Fatal(err)
	}
	if len(delivery.Outputs) != 1 || delivery.Outputs[0].Message != "secret fixture" || delivery.Outputs[0].TargetID != 11001 {
		t.Fatalf("private delivery: %+v", delivery)
	}
	if _, err := m.ClaimPrivate("sealdice", "private", r.PrivateReceipt); !errors.Is(err, ErrAlreadyClaimed) {
		t.Fatalf("second claim should not redeliver: %v", err)
	}
	if err := m.AckPrivate(delivery.DeliveryID, "unknown"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.ClaimPrivate("sealdice", "private", r.PrivateReceipt); !errors.Is(err, ErrNotFound) {
		t.Fatalf("acked plaintext remains: %v", err)
	}
}

func TestRegistrationFencesOldSocketAndRequiresActivation(t *testing.T) {
	m := openManager(t, filepath.Join(t.TempDir(), "bridge.db"))
	conn, err := m.Register("sealdice", "socket-1", RegisterParams{Version: 1, BackendInstance: "app", Capabilities: []string{"reply", "complete"}})
	if err != nil {
		t.Fatal(err)
	}
	if statuses := m.Backends(); len(statuses) != 1 || statuses[0].Ready {
		t.Fatalf("registered socket became ready before ACK: %+v", statuses)
	}
	if _, err := m.Call(context.Background(), groupRequest("not-ready"), func(context.Context, string, string, int32) error { return nil }); !errors.Is(err, ErrBackendUnavailable) {
		t.Fatalf("unacked socket accepted a request: %v", err)
	}
	if err := m.Activate("sealdice", "socket-1", conn); err != nil {
		t.Fatal(err)
	}
	callID := make(chan int32, 1)
	result := make(chan Result, 1)
	go func() {
		r, _ := m.Call(context.Background(), groupRequest("old-socket"), func(_ context.Context, _, _ string, id int32) error { callID <- id; return nil })
		result <- r
	}()
	id := waitMessageID(t, callID)
	newConn, err := m.Register("sealdice", "socket-2", RegisterParams{Version: 1, BackendInstance: "app", Capabilities: []string{"reply", "complete"}})
	if err != nil {
		t.Fatal(err)
	}
	if r := <-result; r.Status != "unknown" {
		t.Fatalf("old request survived registration fence: %+v", r)
	}
	if err := m.Deliver("sealdice", conn, id, "send_group_msg", "group", 22001, "late"); !errors.Is(err, ErrConnectionChanged) {
		t.Fatalf("old socket output accepted: %v", err)
	}
	if err := m.Activate("sealdice", "socket-2", newConn); err != nil {
		t.Fatal(err)
	}
}

func TestCompletionMismatchAndRoutingAreFailClosed(t *testing.T) {
	m := openManager(t, filepath.Join(t.TempDir(), "bridge.db"))
	conn := register(t, m, "socket-1")
	ids := make(chan int32, 1)
	done := make(chan Result, 1)
	go func() {
		r, _ := m.Call(context.Background(), groupRequest("bad-count"), func(_ context.Context, _, _ string, id int32) error { ids <- id; return nil })
		done <- r
	}()
	id := waitMessageID(t, ids)
	if err := m.Deliver("sealdice", conn, id, "send_group_msg", "group", 33002, "wrong group"); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("wrong recipient accepted: %v", err)
	}
	if err := m.Deliver("sealdice", conn, id, "send_group_msg", "group", 22001, "good group"); err != nil {
		t.Fatal(err)
	}
	if err := m.Complete("sealdice", conn, id, "ok", 0); !errors.Is(err, ErrBadCompletion) {
		t.Fatalf("count mismatch accepted: %v", err)
	}
	if r := <-done; r.Status != "unknown" || len(r.Outputs) != 0 {
		t.Fatalf("mismatched completion exposed result: %+v", r)
	}
}

func TestRequestDedupConflictAndPersistenceOnRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bridge.db")
	m := openManager(t, path)
	conn := register(t, m, "socket-1")
	var calls atomic.Int32
	start := make(chan struct{})
	results := make([]Result, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i], errs[i] = m.Call(context.Background(), groupRequest("same"), func(_ context.Context, _, _ string, id int32) error {
				calls.Add(1)
				if err := m.Deliver("sealdice", conn, id, "send_group_msg", "group", 22001, "only once"); err != nil {
					return err
				}
				return m.Complete("sealdice", conn, id, "ok", 1)
			})
		}(i)
	}
	close(start)
	wg.Wait()
	if calls.Load() != 1 || errs[0] != nil || errs[1] != nil || len(results[0].Outputs) != 1 || len(results[1].Outputs) != 1 {
		t.Fatalf("dedup failed calls=%d errs=%v results=%+v", calls.Load(), errs, results)
	}
	conflict := groupRequest("same")
	conflict.Payload = "different"
	if _, err := m.Call(context.Background(), conflict, func(context.Context, string, string, int32) error { return nil }); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("request conflict ignored: %v", err)
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
	register(t, m2, "socket-2")
	var redispatch atomic.Int32
	r, err := m2.Call(context.Background(), groupRequest("same"), func(context.Context, string, string, int32) error { redispatch.Add(1); return nil })
	if err != nil || r.Status != "ok" || redispatch.Load() != 0 {
		t.Fatalf("completed request not restored: result=%+v err=%v redispatch=%d", r, err, redispatch.Load())
	}
	// Simulate a process crash after durable reservation but before completion.
	unknown := groupRequest("crashed")
	if err := m2.writeRequest(requestKey(unknown.BackendID, unknown.RequestID), diskRequest{Fingerprint: fingerprintRequest(unknown), CreatedAt: time.Now().UnixNano(), State: "pending", Result: Result{RequestID: unknown.RequestID, BackendID: unknown.BackendID, Audience: unknown.Audience, Status: "unknown", Outputs: []Output{}}}); err != nil {
		t.Fatal(err)
	}
	if err := m2.Close(); err != nil {
		t.Fatal(err)
	}
	m3, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer m3.Close()
	m3.ConfigureBackends([]string{"sealdice"})
	register(t, m3, "socket-3")
	r, err = m3.Call(context.Background(), unknown, func(context.Context, string, string, int32) error { redispatch.Add(1); return nil })
	if err != nil || r.Status != "unknown" || redispatch.Load() != 0 {
		t.Fatalf("crash request was retried: result=%+v err=%v dispatch=%d", r, err, redispatch.Load())
	}
}

func TestOpaqueIdentityMappingPersistsAndNumericIDsCannotCollide(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bridge.db")
	m := openManager(t, path)
	user, err := m.MapIdentity("sealdice", "user", "sdk-user-1")
	if err != nil {
		t.Fatal(err)
	}
	group, err := m.MapIdentity("sealdice", "group", "sdk-group-1")
	if err != nil {
		t.Fatal(err)
	}
	if user < identityIDFloor || group < identityIDFloor || user == group {
		t.Fatalf("identity mapping outside reserved range: %d %d", user, group)
	}
	if validRequest(Request{BackendID: "sealdice", RequestID: "x", Audience: "private", Payload: "x", UserID: user}) {
		t.Fatal("explicit numeric IDs accepted reserved identity range")
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	m2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()
	user2, err := m2.MapIdentity("sealdice", "user", "sdk-user-1")
	if err != nil || user2 != user {
		t.Fatalf("mapping not persistent: %d %d err=%v", user, user2, err)
	}
}

func TestParseMasterUserKeysFailsClosed(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		ok   bool
		want []string
	}{
		{name: "unset", raw: "", ok: true, want: []string{}},
		{name: "empty", raw: `[]`, ok: true, want: []string{}},
		{name: "valid", raw: `["123:openid_1","987654321:openid-2"]`, ok: true, want: []string{"123:openid_1", "987654321:openid-2"}},
		{name: "non-array", raw: `{"key":"app:openid"}`, want: []string{}},
		{name: "non-string", raw: `[1]`, want: []string{}},
		{name: "missing-app", raw: `[:openid]`, want: []string{}},
		{name: "missing-openid", raw: `["app:"]`, want: []string{}},
		{name: "duplicate", raw: `["123:openid","123:openid","456:another"]`, ok: true, want: []string{"123:openid", "456:another"}},
		{name: "nonnumeric app id", raw: `["app:openid"]`, want: []string{}},
		{name: "spaces rejected", raw: `["123:open id"]`, want: []string{}},
		{name: "colon rejected in openid", raw: `["123:open:id"]`, want: []string{}},
		{name: "newline", raw: `["123:open\nid"]`, want: []string{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := ParseMasterUserKeys(test.raw)
			if ok != test.ok || !reflect.DeepEqual(got, test.want) {
				t.Fatalf("ParseMasterUserKeys(%q) = %#v, %v; want %#v, %v", test.raw, got, ok, test.want, test.ok)
			}
		})
	}
	tooMany := make([]string, 101)
	for i := range tooMany {
		tooMany[i] = fmt.Sprintf("%d:id", i+1)
	}
	raw, err := json.Marshal(tooMany)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := ParseMasterUserKeys(string(raw)); ok || len(got) != 0 {
		t.Fatalf("over-limit identity config was not rejected fail-closed: count=%d valid=%v", len(got), ok)
	}
}

func TestRegistrationCapabilitiesAreBoundedAndUnique(t *testing.T) {
	if !validCapabilities([]string{"reply", "complete", "master-acl-v1"}) {
		t.Fatal("valid capability set rejected")
	}
	if validCapabilities([]string{"reply", "complete", "reply"}) {
		t.Fatal("duplicate capabilities accepted")
	}
	if validCapabilities([]string{"reply", strings.Repeat("x", 65)}) {
		t.Fatal("oversized capability accepted")
	}
	tooMany := make([]string, 33)
	for i := range tooMany {
		tooMany[i] = fmt.Sprintf("cap-%d", i)
	}
	if validCapabilities(tooMany) {
		t.Fatal("over-limit capability set accepted")
	}
}

func TestMasterACLIsNegotiatedAndBoundToBackendConnection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bridge.db")
	m, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	m.ConfigureBackends([]string{"backend-a", "backend-b"})
	m.SetMasterUserKeys([]string{"app:master-openid"})
	t.Cleanup(func() { _ = m.Close() })

	legacy, err := m.Register("backend-a", "legacy-socket", RegisterParams{Version: 1, BackendInstance: "legacy", Capabilities: []string{"reply", "complete"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := m.RegistrationAuthorization("backend-a", legacy); ok {
		t.Fatal("old backend received an ACL without negotiating master-acl-v1")
	}
	if err := m.Activate("backend-a", "legacy-socket", legacy); err != nil {
		t.Fatal(err)
	}

	connA, err := m.Register("backend-a", "acl-socket", RegisterParams{Version: 1, BackendInstance: "acl", Capabilities: []string{"reply", "complete", "master-acl-v1"}})
	if err != nil {
		t.Fatal(err)
	}
	authA, ok := m.RegistrationAuthorization("backend-a", connA)
	if !ok || authA.Version != 1 || len(authA.MasterUserIDs) != 1 {
		t.Fatalf("negotiated ACL missing: %+v %v", authA, ok)
	}
	if err := m.Activate("backend-a", "acl-socket", connA); err != nil {
		t.Fatal(err)
	}
	connB, err := m.Register("backend-b", "other-socket", RegisterParams{Version: 1, BackendInstance: "other", Capabilities: []string{"reply", "complete", "master-acl-v1"}})
	if err != nil {
		t.Fatal(err)
	}
	authB, ok := m.RegistrationAuthorization("backend-b", connB)
	if !ok || len(authB.MasterUserIDs) != 1 || authA.MasterUserIDs[0] == authB.MasterUserIDs[0] {
		t.Fatalf("backend identities were not isolated: A=%+v B=%+v ok=%v", authA, authB, ok)
	}
	if _, ok := m.RegistrationAuthorization("backend-a", legacy); ok {
		t.Fatal("replaced connection retained its old authorization")
	}
	for _, status := range m.Backends() {
		if status.ID == "backend-a" && !hasCapability(status.Capabilities, "master-acl-v1") {
			t.Fatalf("discovery omitted negotiated ACL capability: %+v", status)
		}
		if status.ID == "backend-b" && hasCapability(status.Capabilities, "master-acl-v1") == false {
			t.Fatalf("discovery omitted negotiated ACL capability: %+v", status)
		}
	}
}

func TestEmptyMasterUserListNegotiatesAnEmptyDenyList(t *testing.T) {
	m := openManager(t, filepath.Join(t.TempDir(), "bridge.db"))
	conn, err := m.Register("sealdice", "socket-empty", RegisterParams{Version: 1, BackendInstance: "empty", Capabilities: []string{"reply", "complete", "master-acl-v1"}})
	if err != nil {
		t.Fatal(err)
	}
	authorization, ok := m.RegistrationAuthorization("sealdice", conn)
	if !ok || authorization.Version != 1 || authorization.MasterUserIDs == nil || len(authorization.MasterUserIDs) != 0 {
		t.Fatalf("empty configuration did not return an empty deny list: %+v %v", authorization, ok)
	}
}

func TestMasterACLIDsFollowStableKeysAfterDatabaseReset(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bridge.db")
	openWithACL := func(preallocate int) (*Manager, Authorization) {
		m, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		m.ConfigureBackends([]string{"sealdice"})
		for i := 0; i < preallocate; i++ {
			if _, err := m.MapIdentity("sealdice", "user", fmt.Sprintf("unrelated-%d", i)); err != nil {
				t.Fatal(err)
			}
		}
		m.SetMasterUserKeys([]string{"app:master", "app:other"})
		conn, err := m.Register("sealdice", "socket", RegisterParams{Version: 1, BackendInstance: "instance", Capabilities: []string{"reply", "complete", "master-acl-v1"}})
		if err != nil {
			t.Fatal(err)
		}
		auth, ok := m.RegistrationAuthorization("sealdice", conn)
		if !ok || len(auth.MasterUserIDs) != 2 {
			t.Fatalf("missing identity grants: %+v %v", auth, ok)
		}
		return m, auth
	}
	first, firstAuth := openWithACL(1)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	second, secondAuth := openWithACL(3)
	defer second.Close()
	if firstAuth.MasterUserIDs[0] == secondAuth.MasterUserIDs[0] {
		t.Fatal("fixture expected a different allocation after clearing the identity database")
	}
	for i, key := range []string{"app:master", "app:other"} {
		want, err := second.MapIdentity("sealdice", "user", key)
		if err != nil || secondAuth.MasterUserIDs[i] != want {
			t.Fatalf("authorization id for stable key %q = %d, MapIdentity = %d, err=%v", key, secondAuth.MasterUserIDs[i], want, err)
		}
	}
}

func TestBackendsExposeVersionAndReadiness(t *testing.T) {
	m := openManager(t, filepath.Join(t.TempDir(), "bridge.db"))
	statuses := m.Backends()
	if len(statuses) != 1 || statuses[0].Ready || statuses[0].Version != 0 {
		t.Fatalf("unexpected unregistered status: %+v", statuses)
	}
	register(t, m, "socket-1")
	statuses = m.Backends()
	if len(statuses) != 1 || !statuses[0].Ready || statuses[0].Version != 1 {
		t.Fatalf("unexpected ready status: %+v", statuses)
	}
	if _, err := m.Register("sealdice", "socket-1", RegisterParams{Version: 1, BackendInstance: "app", Capabilities: []string{"reply", "complete"}}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("duplicate socket registration accepted: %v", err)
	}
	if _, err := m.Register("sealdice", "socket-2", RegisterParams{Version: 2, BackendInstance: "app", Capabilities: []string{"reply", "complete"}}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("unsupported registration version accepted: %v", err)
	}
	if _, err := m.Register("sealdice", "socket-2", RegisterParams{Version: 1, BackendInstance: "app", Capabilities: []string{"reply"}}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("incomplete capabilities accepted: %v", err)
	}
}

func TestMessageIDsArePositiveInt32AndNeverReused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bridge.db")
	m := openManager(t, path)
	first, err := m.NextMessageID()
	if err != nil || first != 1 {
		t.Fatalf("first ID=%d err=%v", first, err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	m2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()
	second, err := m2.NextMessageID()
	if err != nil || second != 2 {
		t.Fatalf("second ID=%d err=%v", second, err)
	}
}

func TestRejectedCompletionDoesNotExposePrivateOutbox(t *testing.T) {
	m := openManager(t, filepath.Join(t.TempDir(), "bridge.db"))
	conn := register(t, m, "socket-1")
	ids := make(chan int32, 1)
	done := make(chan Result, 1)
	go func() {
		r, _ := m.Call(context.Background(), groupRequest("private-unknown"), func(_ context.Context, _, _ string, id int32) error { ids <- id; return nil })
		done <- r
	}()
	id := waitMessageID(t, ids)
	if err := m.Deliver("sealdice", conn, id, "send_private_msg", "private", 11001, "secret"); err != nil {
		t.Fatal(err)
	}
	if err := m.Complete("sealdice", conn, id, "ok", 0); !errors.Is(err, ErrBadCompletion) {
		t.Fatal("expected mismatch")
	}
	r := <-done
	if r.Status != "unknown" || r.PrivateReceipt != "" {
		t.Fatalf("unknown result exposed receipt: %+v", r)
	}
	if _, err := m.ClaimPrivate("sealdice", "private-unknown", r.PrivateReceipt); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("unknown outbox claim leaked: %v", err)
	}
}

func TestBackendQueueHasTwentyWaitingSlots(t *testing.T) {
	m := openManager(t, filepath.Join(t.TempDir(), "bridge.db"))
	register(t, m, "socket-1")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	firstStarted := make(chan struct{}, 1)
	release := make(chan struct{})
	for i := 0; i < QueueLimit+1; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _ = m.Call(ctx, groupRequest(fmt.Sprintf("queue-%d", i)), func(callCtx context.Context, _, _ string, _ int32) error {
				select {
				case firstStarted <- struct{}{}:
				case <-callCtx.Done():
					return callCtx.Err()
				}
				select {
				case <-release:
					return nil
				case <-callCtx.Done():
					return callCtx.Err()
				}
			})
		}(i)
	}
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("worker did not start")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		m.mu.Lock()
		pending := 0
		for _, call := range m.records {
			if call.request.BackendID == "sealdice" && call.state != "done" {
				pending++
			}
		}
		m.mu.Unlock()
		if pending >= QueueLimit+1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	// One running plus twenty waiting are accepted; the next call is rejected.
	_, err := m.Call(context.Background(), groupRequest("queue-overflow"), func(context.Context, string, string, int32) error { return nil })
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("queue overflow result: %v", err)
	}
	cancel()
	close(release)
	wg.Wait()
}

func TestDispatchedCancellationQuarantinesUntilExactDrainProof(t *testing.T) {
	m := openManager(t, filepath.Join(t.TempDir(), "bridge.db"))
	conn := register(t, m, "socket-1")
	started := make(chan int32, 1)
	release := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := m.Call(ctx, groupRequest("cancel-drain"), func(_ context.Context, _, _ string, id int32) error {
			started <- id
			<-release // Simulate an application task that outlives the caller context.
			return nil
		})
		done <- err
	}()
	id := waitMessageID(t, started)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("caller did not stop waiting after cancellation")
	}
	waitForReady(t, m, false)
	if _, err := m.Call(context.Background(), groupRequest("cancel-other"), func(context.Context, string, string, int32) error { return nil }); !errors.Is(err, ErrBackendUnavailable) {
		t.Fatalf("quarantined backend accepted a new request: %v", err)
	}
	close(release)
	if err := m.Complete("sealdice", conn, id, "failed", 0); err != nil {
		t.Fatalf("exact late completion did not drain quarantine: %v", err)
	}
	waitForReady(t, m, true)
	// Drain proof changes readiness only. A duplicate call still returns unknown.
	r, err := m.Call(context.Background(), groupRequest("cancel-drain"), func(context.Context, string, string, int32) error {
		t.Fatal("unknown request was redispatched")
		return nil
	})
	if r.Status != "unknown" || err == nil {
		t.Fatalf("late completion changed cached unknown result: result=%+v err=%v", r, err)
	}
}

func TestQueuedUnknownsCannotClearActiveCancellationQuarantine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queued-quarantine.db")
	m, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	m.ConfigureBackends([]string{"sealdice"})
	conn := register(t, m, "socket-1")
	started := make(chan int32, 1)
	release := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() {
		_, callErr := m.Call(ctx, groupRequest("active-cancel"), func(_ context.Context, _, _ string, id int32) error {
			started <- id
			<-release
			return nil
		})
		firstDone <- callErr
	}()
	_ = waitMessageID(t, started)

	queuedResults := make(chan Result, 2)
	for _, requestID := range []string{"queued-after-cancel-1", "queued-after-cancel-2"} {
		requestID := requestID
		go func() {
			result, _ := m.Call(context.Background(), groupRequest(requestID), func(context.Context, string, string, int32) error {
				t.Errorf("quarantined queued request %s was dispatched", requestID)
				return nil
			})
			queuedResults <- result
		}()
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		m.mu.Lock()
		queued := len(m.backends["sealdice"].queue)
		m.mu.Unlock()
		if queued == 2 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	m.mu.Lock()
	queued := len(m.backends["sealdice"].queue)
	m.mu.Unlock()
	if queued != 2 {
		t.Fatalf("expected two queued requests, got %d", queued)
	}

	cancel()
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("canceled call did not return")
	}
	waitForReady(t, m, false)
	for range 2 {
		select {
		case result := <-queuedResults:
			if result.Status != "unknown" {
				t.Fatalf("queued result should be unknown while quarantined: %+v", result)
			}
		case <-time.After(time.Second):
			t.Fatal("queued request did not finish")
		}
	}

	newConn, err := m.Register("sealdice", "socket-2", RegisterParams{Version: 1, BackendInstance: "nonebot-test", Capabilities: []string{"reply", "complete"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Activate("sealdice", "socket-2", newConn); err != nil {
		t.Fatal(err)
	}
	waitForReady(t, m, false)
	if _, err := m.Call(context.Background(), groupRequest("must-stay-blocked"), func(context.Context, string, string, int32) error {
		t.Fatal("quarantined request dispatched")
		return nil
	}); !errors.Is(err, ErrBackendUnavailable) {
		t.Fatalf("queued completions cleared quarantine: %v", err)
	}
	close(release)
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	m2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()
	m2.ConfigureBackends([]string{"sealdice"})
	conn, err = m2.Register("sealdice", "socket-3", RegisterParams{Version: 1, BackendInstance: "nonebot-test", Capabilities: []string{"reply", "complete"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := m2.Activate("sealdice", "socket-3", conn); err != nil {
		t.Fatal(err)
	}
	waitForReady(t, m2, false)
}

func TestQuarantineSurvivesReconnectAndRestartUntilNewInstance(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bridge.db")
	m := openManager(t, path)
	conn := register(t, m, "socket-1")
	started := make(chan int32, 1)
	release := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := m.Call(ctx, groupRequest("restart-quarantine"), func(_ context.Context, _, _ string, id int32) error {
			started <- id
			<-release
			return nil
		})
		done <- err
	}()
	id := waitMessageID(t, started)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("request did not become unknown after cancel")
	}
	waitForReady(t, m, false)
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	close(release)
	m2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	m2.ConfigureBackends([]string{"sealdice"})
	t.Cleanup(func() { _ = m2.Close() })
	sameConn, err := m2.Register("sealdice", "socket-2", RegisterParams{Version: 1, BackendInstance: "nonebot-test", Capabilities: []string{"reply", "complete"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := m2.Activate("sealdice", "socket-2", sameConn); err != nil {
		t.Fatal(err)
	}
	waitForReady(t, m2, false)
	if err := m2.Complete("sealdice", conn, id, "failed", 0); err != nil {
		t.Fatalf("old exact completion should drain persisted quarantine: %v", err)
	}
	waitForReady(t, m2, false) // old socket is gone; do not reactivate new same-instance socket.
	newConn, err := m2.Register("sealdice", "socket-3", RegisterParams{Version: 1, BackendInstance: "nonebot-new-process", Capabilities: []string{"reply", "complete"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := m2.Activate("sealdice", "socket-3", newConn); err != nil {
		t.Fatal(err)
	}
	waitForReady(t, m2, true)
}

func TestExpiredDispatchStillReconstructsQuarantine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "aged.db")
	m := openManager(t, path)
	call := &callState{
		request: groupRequest("aged-dispatch"), fingerprint: fingerprintRequest(groupRequest("aged-dispatch")),
		requestID: 41, connectionID: "old-connection", socketID: "old-socket", backendInstance: "same-process",
		createdAt: time.Now().Add(-RequestTTL - time.Hour).UnixNano(), acceptedCount: 2,
		result: Result{RequestID: "aged-dispatch", BackendID: "sealdice", Audience: "group", Status: "unknown", Outputs: []Output{}},
	}
	if err := m.writeRequest(requestKey("sealdice", "aged-dispatch"), m.diskRecord(call, "dispatching")); err != nil {
		t.Fatal(err)
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
	conn, err := m2.Register("sealdice", "new-socket", RegisterParams{Version: 1, BackendInstance: "same-process", Capabilities: []string{"reply", "complete"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := m2.Activate("sealdice", "new-socket", conn); err != nil {
		t.Fatal(err)
	}
	waitForReady(t, m2, false)
	if _, err := m2.Call(context.Background(), groupRequest("after-aged-crash"), func(context.Context, string, string, int32) error { return nil }); !errors.Is(err, ErrBackendUnavailable) {
		t.Fatalf("expired dispatch cleared quarantine: %v", err)
	}
	if err := m2.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(bucketRequests).Get([]byte(requestKey("sealdice", "aged-dispatch"))) != nil {
			t.Fatal("expired request record should be pruned")
		}
		if tx.Bucket(bucketQuarantine).Get([]byte("sealdice")) == nil {
			t.Fatal("quarantine must outlive request TTL")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func waitForReady(t *testing.T, m *Manager, ready bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		statuses := m.Backends()
		if len(statuses) == 1 && statuses[0].Ready == ready {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("backend readiness did not become %v: %+v", ready, m.Backends())
}
