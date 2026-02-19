package main

import (
	"bufio"
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

	data := resp.NewSafeMap()
	expq := resp.NewSafePriorityQueue()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	go func() {
		for _ = range ticker.C {
			resp.CollectGarbage(data, expq)
		}
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println(err)
			break
		}

		go serve(conn, data, expq)
	}

	return 0
}

func serve(conn net.Conn, data *resp.SafeMap, expq *resp.SafePriorityQueue) {
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

		response, err := resp.ExecuteCommand(request, data, expq)
		if err != nil {
			log.Println(err)
			return
		}

		_, err = conn.Write([]byte(response))
		if err != nil {
			log.Println(err)
			return
		}

		request = resp.NewArray()
	}
}
