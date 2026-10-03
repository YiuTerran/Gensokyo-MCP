package callapi

import (
	"encoding/json"
	"testing"
)

func TestParamsContentRejectsFractionalAndUnsafeNumericIDs(t *testing.T) {
	for _, raw := range []string{
		`{"group_id":1.5}`,
		`{"user_id":9007199254740992}`,
		`{"message_id":1e3}`,
		`{"channel_id":9223372036854775808}`,
	} {
		var params ParamsContent
		if err := json.Unmarshal([]byte(raw), &params); err == nil {
			t.Fatalf("invalid numeric ID accepted: %s", raw)
		}
	}
}

func TestParamsContentPreservesLegacyStringIDsAndIntegralNumbers(t *testing.T) {
	var params ParamsContent
	if err := json.Unmarshal([]byte(`{"group_id":"opaque:00042","user_id":11001}`), &params); err != nil {
		t.Fatal(err)
	}
	if params.GroupID != "opaque:00042" || params.UserID != "11001" {
		t.Fatalf("compatibility IDs changed: %#v %#v", params.GroupID, params.UserID)
	}
}
