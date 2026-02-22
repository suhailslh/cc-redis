package resp

import (
	"io"
)

type Topic struct {
	Channel chan *Array
	Subscribers []io.Writer
}

func NewTopic() *Topic {
	return &Topic{Channel: make(chan *Array, 10)}
}

func NewMessage(msgType string, channel string, msg DataType) *Array {
	message := NewArray()
	message.Len = 3
	message.Value = append(message.Value, NewBulkStringWithValue(msgType), NewBulkStringWithValue(channel), msg)

	return message
}
