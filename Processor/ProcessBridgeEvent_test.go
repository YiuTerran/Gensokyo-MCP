package Processor

import "testing"

func TestBuildBridgeEventPreservesGroupRoleWithoutDefaultingUnknown(t *testing.T) {
	for _, role := range []string{"owner", "admin", "member"} {
		event, err := BuildBridgeEventWithRole(".set coc", "group", 11001, 22001, 1, role)
		if err != nil {
			t.Fatalf("role %q: %v", role, err)
		}
		sender, ok := event["sender"].(map[string]interface{})
		if !ok || sender["role"] != role {
			t.Fatalf("role %q missing from sender: %#v", role, event["sender"])
		}
	}

	event, err := BuildBridgeEvent(".test", "group", 11001, 22001, 2)
	if err != nil {
		t.Fatal(err)
	}
	sender, ok := event["sender"].(map[string]interface{})
	if !ok {
		t.Fatalf("unexpected sender value: %#v", event["sender"])
	}
	if _, exists := sender["role"]; exists {
		t.Fatalf("unknown role was defaulted or emitted: %#v", sender)
	}
}

func TestBuildBridgeEventRejectsInvalidOrPrivateGroupRole(t *testing.T) {
	for _, test := range []struct {
		name, audience, role string
	}{
		{name: "invalid role", audience: "group", role: "moderator"},
		{name: "private role", audience: "private", role: "admin"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := BuildBridgeEventWithRole(".test", test.audience, 11001, 22001, 1, test.role); err == nil {
				t.Fatalf("accepted audience=%q role=%q", test.audience, test.role)
			}
		})
	}
}
