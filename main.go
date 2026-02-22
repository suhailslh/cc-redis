package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/suhailslh/cc-redis/resp"
)

func main() {
	ready := make(chan bool, 1)
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	
	os.Exit(run(ready, interrupt))	
}

func run(ready chan<- bool, interrupt <-chan os.Signal) int {
	listener, err := net.Listen("tcp", ":36245")
	if err != nil {
		log.Println(err)
		return 1
	}

	go func() {
		<-interrupt
		fmt.Println()
		log.Println("Shutting down...")
		listener.Close()
	}()

	log.Printf("Listening to %s", listener.Addr())

	ready <- true

	data := resp.NewSafeMap[string, resp.DataType]()
	expq := resp.NewSafePriorityQueue()
	topics := resp.NewSafeMap[string, *resp.Topic]()
	subscriptions := resp.NewSafeMap[string, int64]()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	go func() {
		for _ = range ticker.C {
			purge(data, expq)
		}
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println(err)
			if errors.Is(err, net.ErrClosed) {
				break
			}
		}

		go serve(conn, data, expq, topics, subscriptions)
	}

	return 0
}

func serve(conn net.Conn, data *resp.SafeMap[string, resp.DataType], expq *resp.SafePriorityQueue, topics *resp.SafeMap[string, *resp.Topic], subscriptions *resp.SafeMap[string, int64]) {
	defer conn.Close()

	request := resp.NewArray()
	
	scanner := bufio.NewScanner(conn)
	scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF {
			return 0, nil, bufio.ErrFinalToken
		}
		
		i := strings.Index(string(data), "\r\n")
		if i == -1 {
			return 0, nil, nil
		}

		if request.Len < -1 {
			request.Len, err = strconv.Atoi(string(data[1:i]))
			if err != nil {
				return 0, nil, err
			}

			return i+2, data, nil
		} else {
			if request.Value == nil || request.Value[len(request.Value) - 1].(*resp.BulkString).IsInitialized() {
				request.Value = append(request.Value, resp.NewBulkString())
			}

			arg := request.Value[len(request.Value) - 1].(*resp.BulkString)
			if arg.Len < -1 {
				arg.Len, err = strconv.Atoi(string(data[1:i]))
				if err != nil {
					return 0, nil, err
				}

				return i+2, data, nil
			} else {
				if len(data) < arg.Len {
					return 0, nil, nil
				}

				arg.Value = string(data[:arg.Len])
				return arg.Len+2, data, nil
			}
		}
	})

	for scanner.Scan() {
		if !request.IsInitialized() {
			continue
		}

		execute(request, data, expq, topics, subscriptions, conn)
		request = resp.NewArray()
	}
}

func execute(request *resp.Array, data *resp.SafeMap[string, resp.DataType], expq *resp.SafePriorityQueue, topics *resp.SafeMap[string, *resp.Topic], subscriptions *resp.SafeMap[string, int64], conn net.Conn) {
	if request.Len < 1 {
		log.Println(fmt.Errorf("Invalid Request %#q", request.String()))
		return
	}

	command := strings.ToUpper(request.Value[0].(*resp.BulkString).Value)
	var response string
	var err error
	switch command {
		case "HELLO","COMMAND","CONFIG":
			response, err = "*2\r\n$5\r\nhello\r\n*1\r\n$5\r\nworld\r\n", nil
		case "CLIENT":
	        	response, err = "+OK\r\n", nil
		case "PING":
			response, err = "+PONG\r\n", nil
		case "SET":
			response, err = resp.Set(request, data, expq)
		case "GET":
			response, err = resp.Get(request, data)
		case "EXISTS":
			response, err = resp.Exists(request, data)
		case "DEL":
			response, err = resp.Del(request, data)
		case "SUBSCRIBE":
			response, err = resp.Subscribe(request, topics, subscriptions, conn.RemoteAddr().String(), conn)
		case "UNSUBSCRIBE":
			response, err = resp.Unsubscribe(request, topics, subscriptions, conn.RemoteAddr().String(), conn)
		case "PUBLISH":
			response, err = resp.Publish(request, topics)
	}

	if err != nil {
		log.Println(err)
		return
	}

	if response == "" {
		return
	}

	_, err = conn.Write([]byte(response))
	if err != nil {
		log.Println(err)
		return
	}
}

func purge(data *resp.SafeMap[string, resp.DataType], expq *resp.SafePriorityQueue) {
	currentTime := time.Now().UnixMilli()
	for top := expq.Peek(); top != nil && top.(resp.PQItem).Value <= currentTime; top = expq.Peek() {
		expq.Pop()
		value, ok := data.Read(top.(resp.PQItem).Key)
		if ok && value != nil && value.(*resp.BulkString).ExpiresAt <= currentTime {
			data.Delete(top.(resp.PQItem).Key)
		}
	}
}
