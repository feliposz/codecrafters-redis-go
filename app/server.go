package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
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
					fmt.Printf("[#%d] Wrong string size - got: %d, want: %d\n", len(token), strSize)
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
			response = fmt.Sprintf("$%d\r\n%s\r\n", len(cmd[1]), cmd[1])
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
