package resp

import (
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"slices"
	"strconv"
	"time"
)

func Echo(request *Array) (string, error) {
	if request.Len != 2 {
		return "", fmt.Errorf("Invalid Request %#q", request.String())
	}
	return request.Value[1].(*BulkString).String(), nil
}

func Set(request *Array, data *SafeMap[string, DataType], expq *SafePriorityQueue) (string, error) {
	if request.Len < 3 {
		return "", fmt.Errorf("Invalid Request %#q", request.String())
	}

	key := request.Value[1].(*BulkString).Value
	value := request.Value[2]
	
	var offset, timestamp int64
	var err error
	for i := 3; i < request.Len; i++ {
		arg := request.Value[i].(*BulkString).Value
		switch arg {
			case "EX":
				if i + 1 >= request.Len {
					return "", fmt.Errorf("Invalid Request %#q", request.String())
				}
				offset, err = strconv.ParseInt(request.Value[i + 1].(*BulkString).Value, 10, 64)
				if err != nil {
					return "", err
				}
				i += 1
				value.(*BulkString).ExpiresAt = time.Now().UnixMilli() + offset * 1000
			case "EXAT":
				if i + 1 >= request.Len {
					return "", fmt.Errorf("Invalid Request %#q", request.String())
				}
				timestamp, err = strconv.ParseInt(request.Value[i + 1].(*BulkString).Value, 10, 64)
				if err != nil {
					return "", err
				}
				i += 1
				value.(*BulkString).ExpiresAt = timestamp * 1000
			case "PX":
				if i + 1 >= request.Len {
					return "", fmt.Errorf("Invalid Request %#q", request.String())
				}
				offset, err = strconv.ParseInt(request.Value[i + 1].(*BulkString).Value, 10, 64)
				if err != nil {
					return "", err
				}
				i += 1
				value.(*BulkString).ExpiresAt = time.Now().UnixMilli() + offset
			case "PXAT":
				if i + 1 >= request.Len {
					return "", fmt.Errorf("Invalid Request %#q", request.String())
				}
				timestamp, err = strconv.ParseInt(request.Value[i + 1].(*BulkString).Value, 10, 64)
				if err != nil {
					return "", err
				}
				i += 1
				value.(*BulkString).ExpiresAt = timestamp
		}
	}
	
	data.Write(key, value)
	if value.(*BulkString).ExpiresAt < math.MaxInt64 {
		expq.Push(PQItem{Key: key, Value: value.(*BulkString).ExpiresAt})
	}

	return "+OK\r\n", nil
}

func Get(request *Array, data *SafeMap[string, DataType]) (string, error) {
	if request.Len != 2 {
		return "", fmt.Errorf("Invalid Request %#q", request.String())
	}

	key := request.Value[1].(*BulkString).Value
	value, ok := data.Read(key)

	if !ok || value == nil || value.(*BulkString).ExpiresAt <= time.Now().UnixMilli() {
		data.Delete(key)
		return "$-1\r\n", nil
	}

	return value.(*BulkString).String(), nil
}

func Exists(request *Array, data *SafeMap[string, DataType]) (string, error) {
	if request.Len < 2 {
		return "", fmt.Errorf("Invalid Request %#q", request.String())
	}

	res := 0
	for i := 1; i < request.Len; i++ {
		key := request.Value[i].(*BulkString).Value
		value, ok := data.Read(key)
		if ok {
			if value == nil || value.(*BulkString).ExpiresAt <= time.Now().UnixMilli() {
				data.Delete(key)
			} else {
				res += 1
			}
		}
	}

	return fmt.Sprintf(":%d\r\n", res), nil
}

func Del(request *Array, data *SafeMap[string, DataType]) (string, error) {
	if request.Len < 2 {
		return "", fmt.Errorf("Invalid Request %#q", request.String())
	}

	res := 0
	for i := 1; i < request.Len; i++ {
		key := request.Value[i].(*BulkString).Value
		if _, ok := data.Read(key); ok {
			data.Delete(key)
			res += 1
		}
	}

	return fmt.Sprintf(":%d\r\n", res), nil
}

func Subscribe(request *Array, topics *SafeMap[string, *Topic], subscriptions *SafeMap[string, int64], remoteAddr string, w io.Writer) (string, error) {
	if request.Len < 2 {
		return "", fmt.Errorf("Invalid Request %#q", request.String())
	}

	for i := 1; i < request.Len; i++ {
		key := request.Value[i].(*BulkString).Value
		topic, ok := topics.Read(key)
		
		if ok {
			if !slices.Contains(topic.Subscribers, w) {
				topic.Subscribers = append(topic.Subscribers, w)
				subscriptionCount, _ := subscriptions.Read(remoteAddr)
				subscriptions.Write(remoteAddr, subscriptionCount+1)
			}
		} else {
			topic = NewTopic()
			topic.Subscribers = append(topic.Subscribers, w)
			
			go func(topic *Topic) {
				for message := range topic.Channel {
					for i := len(topic.Subscribers)-1; i > -1; i-- {
						subscriber := topic.Subscribers[i]
						_, err := subscriber.Write([]byte(message.String()))
						if err != nil {
							log.Println(err)
							if errors.Is(err, net.ErrClosed) {
								topic.Subscribers = slices.Delete(topic.Subscribers, i, i+1)
							}
						}
					}
				}
			}(topic)

			topics.Write(key, topic)
			subscriptionCount, _ := subscriptions.Read(remoteAddr)
			subscriptions.Write(remoteAddr, subscriptionCount+1)
		}

		subscriptionCount, _ := subscriptions.Read(remoteAddr)
		message := NewMessage("subscribe", key, NewIntegerWithValue(subscriptionCount))
		_, err := w.Write([]byte(message.String()))
		if err != nil {
			return "", err
		}
	}

	return "", nil
}

func Unsubscribe(request *Array, topics *SafeMap[string, *Topic], subscriptions *SafeMap[string, int64], remoteAddr string, w io.Writer) (string, error) {
	if request.Len < 2 {
		return "", fmt.Errorf("Invalid Request %#q", request.String())
	}

	for i := 1; i < request.Len; i++ {
		key := request.Value[i].(*BulkString).Value
		topic, ok := topics.Read(key)
		index := slices.Index(topic.Subscribers, w)
		
		if ok && index != -1 {
			topic.Subscribers = slices.Delete(topic.Subscribers, index, index+1)
			subscriptionCount, _ := subscriptions.Read(remoteAddr)
			subscriptions.Write(remoteAddr, subscriptionCount-1)
		}

		subscriptionCount, _ := subscriptions.Read(remoteAddr)
		message := NewMessage("unsubscribe", key, NewIntegerWithValue(subscriptionCount))
		_, err := w.Write([]byte(message.String()))
		if err != nil {
			return "", err
		}
	}

	return "", nil
}

func Publish(request *Array, topics *SafeMap[string, *Topic]) (string, error) {
	if request.Len != 3 {
		return "", fmt.Errorf("Invalid Request %#q", request.String())
	}

	key := request.Value[1].(*BulkString).Value
	topic, ok := topics.Read(key)
	var subscriberCount int64 = 0

	if ok {
		subscriberCount = int64(len(topic.Subscribers))
		message := NewMessage("message", key, request.Value[2])
		topic.Channel <- message
	}

	return NewIntegerWithValue(subscriberCount).String(), nil
}
