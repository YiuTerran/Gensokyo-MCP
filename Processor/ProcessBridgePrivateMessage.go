package Processor

import (
	"fmt"
	"strconv"
	"time"

	"github.com/hoshinonyaruko/gensokyo-mcp/config"
	"github.com/hoshinonyaruko/gensokyo-mcp/handlers"
	"github.com/hoshinonyaruko/gensokyo-mcp/wsclient"
)

// ProcessBridgePrivateMessage emits a single synthetic private event to the selected backend.
func ProcessBridgePrivateMessage(payload, userID string, clients []*wsclient.WebSocketClient) error {
	if userID == "" {
		return fmt.Errorf("user_id is required for private audience")
	}
	if payload == "" {
		payload = "帮助"
	}
	var userValue interface{} = userID
	id, err := NextSyntheticMessageID()
	if err != nil {
		return fmt.Errorf("allocate OneBot message ID: %w", err)
	}
	var messageIDValue interface{} = strconv.FormatInt(int64(id), 10)
	if !config.GetStringOb11() {
		parsed, err := strconv.ParseInt(userID, 10, 64)
		if err != nil {
			return fmt.Errorf("numeric user_id required by string_ob11=false: %w", err)
		}
		userValue = parsed
		messageIDValue = int64(id)
	}
	var message interface{} = payload
	if config.GetArrayValue() {
		message = handlers.ConvertToSegmentedMessage(payload)
	}
	event := map[string]interface{}{
		"raw_message":  payload,
		"message_id":   messageIDValue,
		"message_type": "private",
		"post_type":    "message",
		"self_id":      config.GetUinint64(),
		"user_id":      userValue,
		"sender":       map[string]interface{}{"nickname": "", "user_id": userValue},
		"sub_type":     "friend",
		"font":         0,
		"message_seq":  0,
		"time":         time.Now().Unix(),
		"message":      message,
	}
	if !config.GetNativeOb11() {
		event["real_message_type"] = "group_private"
		event["real_user_id"] = userID
		event["is_binded_user_id"] = false
	}
	return BroadcastMessageToAll(event, clients)
}
