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
var replicas []net.Conn
var replicaOffset int
var ackReceived chan bool

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
		defer masterConn.Close()

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

		// receiving RDB (ignoring it for now)
		response, _ := reader.ReadString('\n')
		if response[0] != '$' {
			fmt.Printf("Invalid response\n")
			os.Exit(1)
		}
		rdbSize, _ := strconv.Atoi(response[1 : len(response)-2])
		buffer := make([]byte, rdbSize)
		receivedSize, err := reader.Read(buffer)
		if err != nil {
			fmt.Printf("Invalid RDB received %v\n", err)
			os.Exit(1)
		}
		if rdbSize != receivedSize {
			fmt.Printf("Size mismatch - got: %d, want: %d\n", receivedSize, rdbSize)
		}

		go handlePropagation(reader, masterConn)
	}

	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", config.port))
	if err != nil {
		fmt.Printf("Failed to bind to port %d\n", config.port)
		os.Exit(1)
	}
	fmt.Println("Listening on: ", listener.Addr().String())

	store = make(map[string]string)
	ttl = make(map[string]time.Time)
	ackReceived = make(chan bool)

	for id := 1; ; id++ {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go serveClient(id, conn)
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

		if len(response) > 0 {
			bytesSent, err := conn.Write([]byte(response))
			if err != nil {
				fmt.Printf("[#%d] Error writing response: %v\n", id, err.Error())
				break
			}
			fmt.Printf("[#%d] Bytes sent: %d %q\n", id, bytesSent, response)
		}

		if resynch {
			emptyRDB := []byte("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
			buffer := make([]byte, hex.DecodedLen(len(emptyRDB)))
			// TODO: check for errors
			hex.Decode(buffer, emptyRDB)
			conn.Write([]byte(fmt.Sprintf("$%d\r\n", len(buffer))))
			conn.Write(buffer)
			fmt.Printf("[#%d] full resynch sent: %d\n", id, len(buffer))
			replicas = append(replicas, conn)
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

func encodeInt(n int) string {
	return fmt.Sprintf(":%d\r\n", n)
}

func handleCommand(cmd []string) (response string, resynch bool) {
	isWrite := false
	switch strings.ToUpper(cmd[0]) {
	case "COMMAND":
		response = "+OK\r\n"
	case "REPLCONF":
		switch strings.ToUpper(cmd[1]) {
		case "GETACK":
			response = encodeStringArray([]string{"REPLCONF", "ACK", strconv.Itoa(replicaOffset)})
		case "ACK":
			ackReceived <- true
			response = ""
		default:
			// TODO: Implement proper replication
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
		isWrite = true
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
	case "WAIT":
		count, _ := strconv.Atoi(cmd[1])
		timeout, _ := strconv.Atoi(cmd[2])
		response = handleWait(count, timeout)
	}
	if isWrite {
		propagate(cmd)
	}
	return
}

func propagate(cmd []string) {
	if len(replicas) == 0 {
		return
	}
	for i := 0; i < len(replicas); i++ {
		fmt.Printf("Replicating to: %s\n", replicas[i].RemoteAddr().String())
		_, err := replicas[i].Write([]byte(encodeStringArray(cmd)))
		// remove stale replicas
		if err != nil {
			fmt.Printf("Disconnected: %s\n", replicas[i].RemoteAddr().String())
			if len(replicas) > 0 {
				last := len(replicas) - 1
				replicas[i] = replicas[last]
				replicas = replicas[:last]
				i--
			}
		}
	}
}

func handleWait(count, timeout int) string {
	propagate([]string{"REPLCONF", "GETACK", "*"})

	timer := time.After(time.Duration(timeout) * time.Millisecond)

	acks := 0
outer:
	for acks < count {
		select {
		case <-ackReceived:
			fmt.Println("acks =", acks)
			acks++
		case <-timer:
			fmt.Println("timeout! acks =", acks)
			break outer
		}
	}
	return encodeInt(acks)
}

func handlePropagation(reader *bufio.Reader, masterConn net.Conn) {
	for {
		cmd := []string{}
		var arrSize, strSize, cmdSize int
		for {
			token, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			// HACK: should count bytes properly?
			cmdSize += len(token)
			token = strings.TrimRight(token, "\r\n")
			// TODO: do proper RESP parsing!!!
			switch {
			case arrSize == 0 && token[0] == '*':
				arrSize, _ = strconv.Atoi(token[1:])
			case strSize == 0 && token[0] == '$':
				strSize, _ = strconv.Atoi(token[1:])
			default:
				if len(token) != strSize {
					fmt.Printf("[from master] Wrong string size - got: %d, want: %d\n", len(token), strSize)
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

		fmt.Printf("[from master] Command = %q\n", cmd)
		response, _ := handleCommand(cmd)
		//fmt.Printf("response = %q\n", response)
		if strings.ToUpper(cmd[0]) == "REPLCONF" {
			//fmt.Printf("ack = %q\n", cmd)
			_, err := masterConn.Write([]byte(response))
			if err != nil {
				fmt.Printf("Error responding to master: %v\n", err.Error())
				break
			}
		}
		replicaOffset += cmdSize
	}
}
