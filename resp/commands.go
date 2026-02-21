package resp

import (
	"fmt"
	"math"
	"strings"
	"strconv"
	"time"
)

func CollectGarbage(data *SafeMap[string, DataType], expq *SafePriorityQueue) {
	currentTime := time.Now().UnixMilli()
	for top := expq.Peek(); top != nil && top.(PQItem).Value <= currentTime; top = expq.Peek() {
		expq.Pop()
		value, ok := data.Read(top.(PQItem).Key)
		if ok && value != nil && value.(*BulkString).ExpiresAt <= currentTime {
			data.Delete(top.(PQItem).Key)
		}
	}
}

func ExecuteCommand(request *Array, data *SafeMap[string, DataType], expq *SafePriorityQueue) (string, error) {
	if request.Len < 1 {
		return "", fmt.Errorf("Invalid Request %#q", request.String())
	}

	command := strings.ToUpper(request.Value[0].(*BulkString).Value)
	switch command {
		case "CLIENT":
			return "+OK\r\n", nil
		case "HELLO","COMMAND":
			return "*2\r\n$5\r\nhello\r\n*1\r\n$5\r\nworld\r\n", nil
		case "PING":
			return "+PONG\r\n", nil
		case "ECHO":
			return echo(request)
		case "SET":
			return set(request, data, expq)
		case "SETEX":
			request.Len = 5
			request.Value[0] = &BulkString{Len: 3, Value: "SET"}
			request.Value[2], request.Value[3] = request.Value[3], request.Value[2]
			request.Value = append(request.Value, &BulkString{Len: 2, Value: "EX"})
			request.Value[3], request.Value[4] = request.Value[4], request.Value[3]
			return set(request, data, expq)
		case "GET":
			return get(request, data)
		case "EXISTS":
			return exists(request, data)
		case "DEL":
			return del(request, data)
		case "WAIT":
			return ":0\r\n", nil
	}

	return request.String(), nil
}

func echo(request *Array) (string, error) {
	if request.Len != 2 {
		return "", fmt.Errorf("Invalid Request %#q", request.String())
	}
	return request.Value[1].(*BulkString).String(), nil
}

func set(request *Array, data *SafeMap[string, DataType], expq *SafePriorityQueue) (string, error) {
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

func get(request *Array, data *SafeMap[string, DataType]) (string, error) {
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

func exists(request *Array, data *SafeMap[string, DataType]) (string, error) {
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

func del(request *Array, data *SafeMap[string, DataType]) (string, error) {
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
