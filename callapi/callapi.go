package callapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/hoshinonyaruko/gensokyo-mcp/mylog"
)

// onebot发来的action调用信息
type ActionMessage struct {
	Action      string        `json:"action"`
	Params      ParamsContent `json:"params"`
	Echo        interface{}   `json:"echo,omitempty"`
	PostType    string        `json:"post_type,omitempty"`
	MessageType string        `json:"message_type,omitempty"`
}

func (a *ActionMessage) UnmarshalJSON(data []byte) error {
	type Alias ActionMessage

	var rawEcho json.RawMessage
	temp := &struct {
		*Alias
		Echo *json.RawMessage `json:"echo,omitempty"`
	}{
		Alias: (*Alias)(a),
		Echo:  &rawEcho,
	}

	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}

	if rawEcho != nil {
		var lastErr error

		var intValue int
		if lastErr = json.Unmarshal(rawEcho, &intValue); lastErr == nil {
			a.Echo = intValue
			return nil
		}

		var strValue string
		if lastErr = json.Unmarshal(rawEcho, &strValue); lastErr == nil {
			a.Echo = strValue
			return nil
		}

		var arrValue []interface{}
		if lastErr = json.Unmarshal(rawEcho, &arrValue); lastErr == nil {
			a.Echo = arrValue
			return nil
		}

		var objValue map[string]interface{}
		if lastErr = json.Unmarshal(rawEcho, &objValue); lastErr == nil {
			a.Echo = objValue
			return nil
		}

		return fmt.Errorf("unable to unmarshal echo: %v", lastErr)
	}

	return nil
}

// params类型
type ParamsContent struct {
	BotQQ       string      `json:"botqq,omitempty"`
	ChannelID   interface{} `json:"channel_id,omitempty"`
	GuildID     interface{} `json:"guild_id,omitempty"`
	GroupID     interface{} `json:"group_id,omitempty"` // 每一种onebotv11实现的字段类型都可能不同
	MessageType string      `json:"message_type,omitempty"`
	MessageID   interface{} `json:"message_id,omitempty"` // 用于撤回信息
	Message     interface{} `json:"message,omitempty"`    // 这里使用interface{}因为它可能是多种类型
	Messages    interface{} `json:"messages,omitempty"`   // 坑爹转发信息
	UserID      interface{} `json:"user_id,omitempty"`    // 这里使用interface{}因为它可能是多种类型
	Duration    int         `json:"duration,omitempty"`   // 可选的整数
	Enable      bool        `json:"enable,omitempty"`     // 可选的布尔值
	// handle quick operation
	Context   Context   `json:"context,omitempty"`   // context 字段
	Operation Operation `json:"operation,omitempty"` // operation 字段
}

// Context 结构体用于存储 context 字段相关信息
type Context struct {
	Avatar      string `json:"avatar,omitempty"`       // 用户头像链接
	Font        int    `json:"font,omitempty"`         // 字体（假设是整数类型）
	MessageID   int    `json:"message_id,omitempty"`   // 消息 ID
	MessageSeq  int    `json:"message_seq,omitempty"`  // 消息序列号
	MessageType string `json:"message_type,omitempty"` // 消息类型
	PostType    string `json:"post_type,omitempty"`    // 帖子类型
	SubType     string `json:"sub_type,omitempty"`     // 子类型
	Time        int64  `json:"time,omitempty"`         // 时间戳
	UserID      int    `json:"user_id,omitempty"`      // 用户 ID
	GroupID     int    `json:"group_id,omitempty"`     // 群号
}

// Operation 结构体用于存储 operation 字段相关信息
type Operation struct {
	Reply    string `json:"reply,omitempty"`     // 回复内容
	AtSender bool   `json:"at_sender,omitempty"` // 是否 @ 发送者
}

// 自定义一个ParamsContent的UnmarshalJSON 让GroupID同时兼容str和int
func (p *ParamsContent) UnmarshalJSON(data []byte) error {
	type Alias ParamsContent
	aux := &struct {
		GroupID   json.RawMessage `json:"group_id"`
		UserID    json.RawMessage `json:"user_id"`
		MessageID json.RawMessage `json:"message_id"`
		ChannelID json.RawMessage `json:"channel_id"`
		GuildID   json.RawMessage `json:"guild_id"`
		*Alias
	}{
		Alias: (*Alias)(p),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	var err error
	if p.GroupID, err = decodeCompatID(aux.GroupID, "group_id"); err != nil {
		return err
	}
	if p.UserID, err = decodeCompatID(aux.UserID, "user_id"); err != nil {
		return err
	}
	if p.MessageID, err = decodeCompatID(aux.MessageID, "message_id"); err != nil {
		return err
	}
	if p.ChannelID, err = decodeCompatID(aux.ChannelID, "channel_id"); err != nil {
		return err
	}
	if p.GuildID, err = decodeCompatID(aux.GuildID, "guild_id"); err != nil {
		return err
	}

	return nil
}

func decodeCompatID(raw json.RawMessage, field string) (interface{}, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return "", nil
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return text, nil // retain legacy opaque string identifiers exactly
	}
	var number json.Number
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&number); err != nil {
		return nil, fmt.Errorf("%s has unsupported type", field)
	}
	value, err := strconv.ParseInt(string(number), 10, 64)
	const maxSafe = int64(9_007_199_254_740_991)
	if err != nil || value > maxSafe || value < -maxSafe {
		return nil, fmt.Errorf("%s must be a safe integer", field)
	}
	return strconv.FormatInt(value, 10), nil
}

// Message represents a standardized structure for the incoming messages.
type Message struct {
	Action string                 `json:"action"`
	Params map[string]interface{} `json:"params"`
	Echo   interface{}            `json:"echo,omitempty"`
}

// 这是一个接口,在wsclient传入client但不需要引用wsclient包,避免循环引用,复用wsserver和client逻辑
type Client interface {
	SendMessage(message map[string]interface{}) error
}

// 为了解决processor和server循环依赖设计的接口
type WebSocketServerClienter interface {
	SendMessage(message map[string]interface{}) error
	Close() error
}

// 根据action订阅handler处理api
type HandlerFunc func(client Client, messgae ActionMessage) (string, error)

var handlers = make(map[string]HandlerFunc)

// RegisterHandler registers a new handler for a specific action.
func RegisterHandler(action string, handler HandlerFunc) {
	handlers[action] = handler
}

// CallAPIFromDict 处理信息 by calling the 对应的 handler.
func CallAPIFromDict(client Client, message ActionMessage) string {
	handler, ok := handlers[message.Action]
	if !ok {
		mylog.Println("Unsupported action:", message.Action)
		return ""
	}

	jsonString, err := handler(client, message)
	if err != nil {
		// 处理错误
		mylog.Println("Error handling action:", message.Action, "Error:", err)
		return ""
	}

	return jsonString
}
