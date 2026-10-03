package main

import (
	"encoding/json"
	"testing"

	"github.com/hoshinonyaruko/gensokyo-mcp/config"
)

func TestApplyOneBotEnvironmentResolvesBridgeSelfID(t *testing.T) {
	oldBridgeEnabled := bridgeEnabled
	bridgeEnabled = true
	t.Cleanup(func() { bridgeEnabled = oldBridgeEnabled })
	t.Setenv("ONEBOT_WS_URL", "")
	t.Setenv("ONEBOT_BACKEND_ID", "")
	t.Setenv("ONEBOT_WS_TOKEN", "")

	tests := []struct {
		name        string
		configured  int64
		environment string
		want        int64
		wantError   bool
	}{
		{name: "default for generated config", configured: 0, want: defaultBridgeSelfID},
		{name: "preserve configured identity", configured: 54321, want: 54321},
		{name: "environment override", configured: 0, environment: "7654321", want: 7654321},
		{name: "negative config rejected", configured: -1, environment: "7654321", wantError: true},
		{name: "zero override rejected", configured: 0, environment: "0", wantError: true},
		{name: "signed override rejected", configured: 0, environment: "+123", wantError: true},
		{name: "fractional override rejected", configured: 0, environment: "12.5", wantError: true},
		{name: "unsafe override rejected", configured: 0, environment: "9007199254740992", wantError: true},
		{name: "unsafe config rejected", configured: maxSafeOneBotID + 1, wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("ONEBOT_SELF_ID", test.environment)
			conf := &config.Config{}
			conf.Settings.Uin = test.configured
			err := applyOneBotEnvironment(conf)
			if (err != nil) != test.wantError {
				t.Fatalf("applyOneBotEnvironment error = %v, wantError %v", err, test.wantError)
			}
			if err == nil && conf.Settings.Uin != test.want {
				t.Fatalf("resolved self ID = %d, want %d", conf.Settings.Uin, test.want)
			}
		})
	}
}

func TestApplyOneBotEnvironmentLeavesNonBridgeIdentityAlone(t *testing.T) {
	oldBridgeEnabled := bridgeEnabled
	bridgeEnabled = false
	t.Cleanup(func() { bridgeEnabled = oldBridgeEnabled })
	t.Setenv("ONEBOT_SELF_ID", "invalid")
	conf := &config.Config{}
	if err := applyOneBotEnvironment(conf); err != nil {
		t.Fatal(err)
	}
	if conf.Settings.Uin != 0 {
		t.Fatalf("non-bridge self ID changed to %d", conf.Settings.Uin)
	}
}

func TestParseCompatOneBotIDAcceptsCanonicalStringsAndSafeIntegers(t *testing.T) {
	tests := []struct {
		name string
		raw  json.RawMessage
		want int64
	}{
		{name: "string", raw: json.RawMessage(`"12345"`), want: 12345},
		{name: "number", raw: json.RawMessage(`12345`), want: 12345},
		{name: "largest safe string", raw: json.RawMessage(`"9007199254740991"`), want: maxSafeOneBotID},
		{name: "largest safe number", raw: json.RawMessage(`9007199254740991`), want: maxSafeOneBotID},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseCompatOneBotID(test.raw)
			if err != nil || got != test.want {
				t.Fatalf("parseCompatOneBotID(%s) = %d, %v; want %d", test.raw, got, err, test.want)
			}
		})
	}
}

func TestParseCompatOneBotIDRejectsNoncanonicalValues(t *testing.T) {
	for _, raw := range []string{
		`""`, `"0"`, `0`, `null`, `"01"`, `01`, `"+1"`, `+1`, `"-1"`, `-1`,
		`"1.0"`, `1.0`, `1e2`, `"1e2"`, `9007199254740992`, `"9007199254740992"`, `true`, `{}`,
	} {
		t.Run(raw, func(t *testing.T) {
			if got, err := parseCompatOneBotID(json.RawMessage(raw)); err == nil {
				t.Fatalf("parseCompatOneBotID(%s) = %d, want error", raw, got)
			}
		})
	}
}

func TestCompatOneBotIDSchemaAdvertisesNumbersAndStrings(t *testing.T) {
	schema := compatOneBotIDSchema("user id")
	choices, ok := schema["oneOf"].([]any)
	if !ok || len(choices) != 2 {
		t.Fatalf("compatibility ID schema lacks both alternatives: %#v", schema)
	}
	got := map[string]bool{}
	for _, choice := range choices {
		property, ok := choice.(map[string]any)
		if !ok {
			t.Fatalf("unexpected compatibility ID schema entry: %#v", choice)
		}
		typ, ok := property["type"].(string)
		if !ok {
			t.Fatalf("schema entry missing type: %#v", property)
		}
		got[typ] = true
		if typ == "integer" && property["maximum"] != maxSafeOneBotID {
			t.Fatalf("integer schema maximum = %#v, want %d", property["maximum"], maxSafeOneBotID)
		}
	}
	if !got["integer"] || !got["string"] {
		t.Fatalf("compatibility ID schema does not advertise integer and string: %#v", got)
	}
}
