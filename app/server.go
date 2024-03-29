package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	listener, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	fmt.Println("Listening on: ", listener.Addr().String())

	for id := 1; ; id++ {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go serveClient(id, conn)
	}
}

func serveClient(id int, conn net.Conn) {
	defer conn.Close()
	fmt.Printf("[#%d] Client connected: %v\n", id, conn.RemoteAddr().String())

	store := make(map[string]string)
	ttl := make(map[string]time.Time)

	for {
		scanner := bufio.NewScanner(conn)

		cmd := []string{}
		var arrSize, strSize int
		for scanner.Scan() {
			token := scanner.Text()
			switch token[0] {
			case '*':
				arrSize, _ = strconv.Atoi(token[1:])
			case '$':
				strSize, _ = strconv.Atoi(token[1:])
			default:
				if len(token) != strSize {
					fmt.Printf("[#%d] Wrong string size - got: %d, want: %d\n", id, len(token), strSize)
					break
				}
				arrSize--
				strSize = 0
				cmd = append(cmd, token)
			}
			if arrSize == 0 {
				break
			}
		}

		// TODO: handle scanner errors

		if len(cmd) == 0 {
			break
		}

		var response string
		switch strings.ToUpper(cmd[0]) {
		case "COMMAND":
			response = "+OK\r\n"
		case "PING":
			response = "+PONG\r\n"
		case "ECHO":
			response = encodeBulkString(cmd[1])
		case "SET":
			// TODO: check length
			key, value := cmd[1], cmd[2]
			store[key] = value
			if len(cmd) == 5 && strings.ToUpper(cmd[3]) == "PX" {
				expiration, _ := strconv.Atoi(cmd[4])
				ttl[key] = time.Now().Add(time.Millisecond * time.Duration(expiration))
			}
			response = "+OK\r\n"
		case "GET":
			// TODO: check length
			key := cmd[1]
			value, ok := store[key]
			if ok {
				expiration, exists := ttl[key]
				if !exists || expiration.After(time.Now()) {
					response = encodeBulkString(value)
				} else if exists {
					delete(ttl, key)
					delete(store, key)
					response = encodeBulkString("")
				}
			} else {
				response = encodeBulkString("")
			}
		}

		bytesSent, err := conn.Write([]byte(response))
		if err != nil {
			fmt.Printf("[#%d] Error writing response: %v\n", id, err.Error())
			break
		}
		fmt.Printf("[#%d] Bytes sent: %d\n", id, bytesSent)
	}

	fmt.Printf("[#%d] Client closing\n", id)
}

func encodeBulkString(s string) string {
	if len(s) == 0 {
		return "$-1\r\n"
	}
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}
