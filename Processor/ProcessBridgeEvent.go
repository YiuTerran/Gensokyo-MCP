package Processor

import (
	"fmt"
	"time"

	"github.com/hoshinonyaruko/gensokyo-mcp/config"
	"github.com/hoshinonyaruko/gensokyo-mcp/handlers"
)

// BuildBridgeEvent creates one ordinary OneBot v11 message event with a
// manager-allocated positive int32 source ID and numeric account identifiers.
func BuildBridgeEvent(payload string, audience string, userID, groupID int64, sourceID int32) (map[string]interface{}, error) {
	return BuildBridgeEventWithRole(payload, audience, userID, groupID, sourceID, "")
}

// BuildBridgeEventWithRole builds the bridge event and attaches the trusted
// group role snapshot when available. An empty role remains unknown.
func BuildBridgeEventWithRole(payload string, audience string, userID, groupID int64, sourceID int32, groupRole string) (map[string]interface{}, error) {
	if payload == "" || (audience != "group" && audience != "private") || userID <= 0 || sourceID <= 0 {
		return nil, fmt.Errorf("invalid bridge event")
	}
	if audience == "private" && groupRole != "" {
		return nil, fmt.Errorf("private bridge event cannot include a group role")
	}
	if audience == "group" && groupRole != "" && !validBridgeGroupRole(groupRole) {
		return nil, fmt.Errorf("invalid group role")
	}
	var message interface{} = payload
	if config.GetArrayValue() {
		message = handlers.ConvertToSegmentedMessage(payload)
	}
	event := map[string]interface{}{
		"raw_message":  payload,
		"message_id":   int64(sourceID),
		"message_type": audience,
		"post_type":    "message",
		"self_id":      config.GetUinint64(),
		"user_id":      userID,
		"sender":       map[string]interface{}{"nickname": "", "user_id": userID},
		"sub_type":     "friend",
		"font":         0,
		"message_seq":  0,
		"time":         time.Now().Unix(),
		"message":      message,
	}
	if audience == "group" {
		if groupID <= 0 {
			return nil, fmt.Errorf("invalid group ID")
		}
		event["group_id"] = groupID
		event["sub_type"] = "normal"
		sender := map[string]interface{}{"nickname": "", "user_id": userID, "sex": "0", "age": 0, "area": "0", "level": "0"}
		if groupRole != "" {
			sender["role"] = groupRole
		}
		event["sender"] = sender
	}
	return event, nil
}

func validBridgeGroupRole(role string) bool {
	switch role {
	case "owner", "admin", "member":
		return true
	default:
		return false
	}
}
