package main

import (
	"bufio"
	"encoding/hex"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type serverConfig struct {
	port          int
	role          string
	replid        string
	replOffset    int
	replicaofHost string
	replicaofPort int
}

var store map[string]string
var ttl map[string]time.Time
var config serverConfig

func main() {

	flag.IntVar(&config.port, "port", 6379, "listen on specified port")
	flag.StringVar(&config.replicaofHost, "replicaof", "", "start server in replica mode of given host and port")
	flag.Parse()

	if len(config.replicaofHost) == 0 {
		config.role = "master"
		config.replid = randReplid()
	} else {
		config.role = "slave"
		switch flag.NArg() {
		case 0:
			config.replicaofPort = 6379
		case 1:
			config.replicaofPort, _ = strconv.Atoi(flag.Arg(0))
		default:
			flag.Usage()
		}
	}

	if config.role == "slave" {
		masterConn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", config.replicaofHost, config.replicaofPort))
		if err != nil {
			fmt.Printf("Failed to connect to master %v\n", err)
			os.Exit(1)
		}
		//defer masterConn.Close()

		// TODO: check responses
		reader := bufio.NewReader(masterConn)
		masterConn.Write([]byte(encodeStringArray([]string{"PING"})))
		reader.ReadString('\n')
		masterConn.Write([]byte(encodeStringArray([]string{"REPLCONF", "listening-port", strconv.Itoa(config.port)})))
		reader.ReadString('\n')
		masterConn.Write([]byte(encodeStringArray([]string{"REPLCONF", "capa", "psync2"})))
		reader.ReadString('\n')
		masterConn.Write([]byte(encodeStringArray([]string{"PSYNC", "?", "-1"})))
		reader.ReadString('\n')

		masterConn.Close()
	}

	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", config.port))
	if err != nil {
		fmt.Printf("Failed to bind to port %d\n", config.port)
		os.Exit(1)
	}
	fmt.Println("Listening on: ", listener.Addr().String())

	store = make(map[string]string)
	ttl = make(map[string]time.Time)

	for id := 1; ; id++ {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go serveClient(id, conn, config)
	}
}

func randReplid() string {
	chars := []byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	result := make([]byte, 40)
	for i := range result {
		c := rand.Intn(len(chars))
		result[i] = chars[c]
	}
	return string(result)
}

func serveClient(id int, conn net.Conn, config serverConfig) {
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

		fmt.Printf("[#%d] Command = %v\n", id, cmd)
		response, resynch := handleCommand(cmd)

		bytesSent, err := conn.Write([]byte(response))
		if err != nil {
			fmt.Printf("[#%d] Error writing response: %v\n", id, err.Error())
			break
		}
		fmt.Printf("[#%d] Bytes sent: %d %q\n", id, bytesSent, response)

		if resynch {
			emptyRDB := []byte("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
			buffer := make([]byte, hex.DecodedLen(len(emptyRDB)))
			// TODO: check for errors
			hex.Decode(buffer, emptyRDB)
			conn.Write([]byte(fmt.Sprintf("$%d\r\n", len(buffer))))
			conn.Write(buffer)
			fmt.Printf("[#%d] full resynch sent: %d\n", id, len(buffer))
		}
	}

	fmt.Printf("[#%d] Client closing\n", id)
}

func encodeBulkString(s string) string {
	if len(s) == 0 {
		return "$-1\r\n"
	}
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

func encodeStringArray(arr []string) string {
	result := fmt.Sprintf("*%d\r\n", len(arr))
	for _, s := range arr {
		result += encodeBulkString(s)
	}
	return result
}

func handleCommand(cmd []string) (response string, resynch bool) {
	switch strings.ToUpper(cmd[0]) {
	case "COMMAND":
		response = "+OK\r\n"
	case "REPLCONF":
		if len(cmd) == 3 {
			// TODO: Implement replication
			response = "+OK\r\n"
		}
	case "PSYNC":
		if len(cmd) == 3 {
			// TODO: Implement synch
			response = fmt.Sprintf("+FULLRESYNC %s 0\r\n", config.replid)
			resynch = true
		}
	case "PING":
		response = "+PONG\r\n"
	case "ECHO":
		response = encodeBulkString(cmd[1])
	case "INFO":
		if len(cmd) == 2 && strings.ToUpper(cmd[1]) == "REPLICATION" {
			response = encodeBulkString(fmt.Sprintf("role:%s\r\nmaster_replid:%s\r\nmaster_repl_offset:%d",
				config.role, config.replid, config.replOffset))
		}
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
	return
}
