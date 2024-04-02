package main

import (
	"bufio"
	"encoding/hex"
	"flag"
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"slices"
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
	dir           string
	dbfilename    string
}

// TODO: add some mutexes around these...

type serverState struct {
	streams       map[string]*stream
	store         map[string]string
	ttl           map[string]time.Time
	config        serverConfig
	replicas      []replica
	replicaOffset int
	ackReceived   chan bool
}

func main() {

	var config serverConfig

	flag.IntVar(&config.port, "port", 6379, "listen on specified port")
	flag.StringVar(&config.replicaofHost, "replicaof", "", "start server in replica mode of given host and port")
	flag.StringVar(&config.dir, "dir", "", "directory where RDB files are stored")
	flag.StringVar(&config.dbfilename, "dbfilename", "", "name of the RDB file")
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

	srv := newServer(config)

	srv.start()
}

func newServer(config serverConfig) *serverState {
	var srv serverState
	srv.store = make(map[string]string)
	srv.ttl = make(map[string]time.Time)
	srv.streams = make(map[string]*stream)
	srv.ackReceived = make(chan bool)
	srv.config = config
	return &srv
}

func (srv *serverState) start() {
	if srv.config.role == "slave" {
		srv.replicaHandshake()
	}

	if len(srv.config.dir) > 0 && len(srv.config.dbfilename) > 0 {
		rdbPath := filepath.Join(srv.config.dir, srv.config.dbfilename)
		err := readRDB(rdbPath, srv.store, srv.ttl)
		if err != nil {
			fmt.Printf("Failed to load '%s': %v\n", rdbPath, err)
		}
	}

	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", srv.config.port))
	if err != nil {
		fmt.Printf("Failed to bind to port %d\n", srv.config.port)
		os.Exit(1)
	}
	fmt.Println("Listening on: ", listener.Addr().String())

	for id := 1; ; id++ {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go srv.serveClient(id, conn)
	}
}

func (srv *serverState) serveClient(id int, conn net.Conn) {
	fmt.Printf("[#%d] Client connected: %v\n", id, conn.RemoteAddr().String())

	scanner := bufio.NewScanner(conn)

	for {
		cmd := []string{}
		var arrSize, strSize int
		fmt.Printf("[%d] Waiting command\n", id)
		for scanner.Scan() {
			token := scanner.Text()
			//fmt.Printf("[%d] Token: %q\n", id, token)
			switch {
			case arrSize == 0 && token[0] == '*':
				arrSize, _ = strconv.Atoi(token[1:])
			case strSize == 0 && token[0] == '$':
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

		fmt.Printf("[#%d] Command = %q\n", id, cmd)
		response, resynch := srv.handleCommand(cmd)

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
			srv.replicas = append(srv.replicas, replica{conn, 0, 0})
			fmt.Printf("[#%d] Client promoted to replica\n", id)
			return
		}
	}

	fmt.Printf("[#%d] Client closing\n", id)
	conn.Close()
}

func (srv *serverState) handleCommand(cmd []string) (response string, resynch bool) {
	isWrite := false

	switch strings.ToUpper(cmd[0]) {
	case "COMMAND":
		response = "+OK\r\n"

	case "REPLCONF":
		switch strings.ToUpper(cmd[1]) {
		case "GETACK":
			response = encodeStringArray([]string{"REPLCONF", "ACK", strconv.Itoa(srv.replicaOffset)})
		case "ACK":
			srv.ackReceived <- true
			response = ""
		default:
			// TODO: Implement proper replication
			response = "+OK\r\n"
		}

	case "PSYNC":
		if len(cmd) == 3 {
			// TODO: Implement synch
			response = fmt.Sprintf("+FULLRESYNC %s 0\r\n", srv.config.replid)
			resynch = true
		}

	case "PING":
		response = "+PONG\r\n"

	case "ECHO":
		response = encodeBulkString(cmd[1])

	case "INFO":
		if len(cmd) == 2 && strings.ToUpper(cmd[1]) == "REPLICATION" {
			response = encodeBulkString(fmt.Sprintf("role:%s\r\nmaster_replid:%s\r\nmaster_repl_offset:%d",
				srv.config.role, srv.config.replid, srv.config.replOffset))
		}

	case "SET":
		isWrite = true
		// TODO: check length
		key, value := cmd[1], cmd[2]
		srv.store[key] = value
		if len(cmd) == 5 && strings.ToUpper(cmd[3]) == "PX" {
			expiration, _ := strconv.Atoi(cmd[4])
			srv.ttl[key] = time.Now().Add(time.Millisecond * time.Duration(expiration))
		}
		response = "+OK\r\n"

	case "GET":
		// TODO: check length
		key := cmd[1]
		value, ok := srv.store[key]
		if ok {
			expiration, exists := srv.ttl[key]
			if !exists || expiration.After(time.Now()) {
				response = encodeBulkString(value)
			} else if exists {
				delete(srv.ttl, key)
				delete(srv.store, key)
				response = encodeBulkString("")
			}
		} else {
			response = encodeBulkString("")
		}

	case "WAIT":
		count, _ := strconv.Atoi(cmd[1])
		timeout, _ := strconv.Atoi(cmd[2])
		response = srv.handleWait(count, timeout)

	case "CONFIG":
		switch cmd[2] {
		case "dir":
			response = encodeStringArray([]string{"dir", srv.config.dir})
		case "dbfilename":
			response = encodeStringArray([]string{"dbfilename", srv.config.dbfilename})
		}

	case "KEYS":
		keys := make([]string, 0, len(srv.store))
		for key := range srv.store {
			keys = append(keys, key)
		}
		response = encodeStringArray(keys)

	case "TYPE":
		key := cmd[1]
		_, exists := srv.streams[key]
		if exists {
			response = encodeSimpleString("stream")
		} else {
			_, exists := srv.store[key]
			if exists {
				response = encodeSimpleString("string")
			} else {
				response = encodeSimpleString("none")
			}
		}

	case "XADD":
		streamKey := cmd[1]
		id := cmd[2]

		// TODO: check parameters

		stream, exists := srv.streams[streamKey]
		if !exists {
			stream = newStream()
			srv.streams[streamKey] = stream
		}

		entry, err := stream.addStreamEntry(id)
		if err != nil {
			response = encodeError(err)
		} else {
			for i := 3; i < len(cmd); i += 2 {
				key, value := cmd[i], cmd[i+1]
				entry.store = append(entry.store, key, value)
			}
			response = encodeBulkString(fmt.Sprintf("%d-%d", entry.id[0], entry.id[1]))
		}

		// notify blocked reads
		for _, ch := range stream.blocked {
			*ch <- true
		}

	case "XRANGE":
		streamKey := cmd[1]
		start := cmd[2]
		end := cmd[3]

		stream, exists := srv.streams[streamKey]
		if !exists || len(stream.entries) == 0 {
			response = "*0\r\n"
			return
		}

		var startIndex, endIndex int

		if start == "-" {
			startIndex = 0
		} else {
			startMs, startSeq, startHasSeq, _ := stream.splitID(start)
			if !startHasSeq {
				startSeq = 0
			}
			startIndex = searchStreamEntries(stream.entries, startMs, startSeq, 0, len(stream.entries)-1)
		}

		if end == "+" {
			endIndex = len(stream.entries) - 1
		} else {
			endMs, endSeq, endHasSeq, _ := stream.splitID(end)
			if !endHasSeq {
				endSeq = math.MaxUint64
			}
			endIndex = searchStreamEntries(stream.entries, endMs, endSeq, startIndex, len(stream.entries)-1)
			if endIndex >= len(stream.entries) {
				endIndex = len(stream.entries) - 1
			}
		}

		// TODO: use a string builder
		entriesCount := endIndex - startIndex + 1
		response = fmt.Sprintf("*%d\r\n", entriesCount)
		for index := startIndex; index <= endIndex; index++ {
			entry := stream.entries[index]
			id := fmt.Sprintf("%d-%d", entry.id[0], entry.id[1])
			response += fmt.Sprintf("*2\r\n$%d\r\n%s\r\n", len(id), id)
			response += fmt.Sprintf("*%d\r\n", len(entry.store))
			for _, kv := range entry.store {
				response += encodeBulkString(kv)
			}
		}

	case "XREAD":
		// TODO: check parameters

		// NOTE: skipped "xread streams"
		readKeyIndex := 2

		isBlocking := false
		blockTimeout := 0
		if cmd[1] == "block" {
			isBlocking = true
			blockTimeout, _ = strconv.Atoi(cmd[2])
			readKeyIndex += 2
		}
		_ = blockTimeout

		readCount := (len(cmd) - readKeyIndex) / 2
		readStartIndex := readKeyIndex + readCount

		readParams := []struct{ key, start string }{}
		for i := 0; i < readCount; i++ {
			streamKey := cmd[i+readKeyIndex]
			start := cmd[i+readStartIndex]

			_, exists := srv.streams[streamKey]
			if exists {
				readParams = append(readParams, struct{ key, start string }{streamKey, start})
			}
		}

		// TODO: use a string builder

		response = fmt.Sprintf("*%d\r\n", len(readParams))

		for _, readParam := range readParams {

			streamKey, start := readParam.key, readParam.start

			// stream key + entry (below)
			response += fmt.Sprintf("*%d\r\n", 2)
			response += encodeBulkString(streamKey)

			stream := srv.streams[streamKey]

			var startMs, startSeq uint64
			var startHasSeq bool

			if start == "$" {
				startMs, startSeq = stream.last[0], stream.last[1]
			} else {
				startMs, startSeq, startHasSeq, _ = stream.splitID(start)
				if !startHasSeq {
					startSeq = 0
				}
			}

			var entry *streamEntry
			var startIndex int

			for entry == nil {
				startIndex = searchStreamEntries(stream.entries, startMs, startSeq, startIndex, len(stream.entries)-1)

				if startIndex < len(stream.entries) {
					entry = stream.entries[startIndex]
				}

				// if found exact match, need to get the next one (xread bound is exclusive)
				if entry != nil && entry.id[0] == startMs && entry.id[1] == startSeq {
					if startIndex+1 < len(stream.entries) {
						entry = stream.entries[startIndex+1]
					} else {
						entry = nil
					}
				}

				if entry == nil {
					if isBlocking {
						waitForAdd := make(chan bool)
						stream.blocked = append(stream.blocked, &waitForAdd)
						timedOut := false
						if blockTimeout > 0 {
							fmt.Printf("Waiting for a write on stream %s (timeout = %d ms)...\n", streamKey, blockTimeout)
							timer := time.After(time.Duration(blockTimeout) * time.Millisecond)
							select {
							case <-waitForAdd:
								timedOut = false
							case <-timer:
								timedOut = true
							}
						} else {
							fmt.Printf("Waiting for a write on stream %s (no timeout!)...\n", streamKey)
							<-waitForAdd
						}
						stream.blocked = slices.DeleteFunc(stream.blocked, func(ch *chan bool) bool { return ch == &waitForAdd })
						if timedOut {
							response = "$-1\r\n"
							return
						}
					} else {
						break
					}
				}
			}

			if entry == nil {
				response = "*0\r\n"
				return
			}

			// single entry
			response += "*1\r\n"
			id := fmt.Sprintf("%d-%d", entry.id[0], entry.id[1])
			response += fmt.Sprintf("*2\r\n$%d\r\n%s\r\n", len(id), id)
			response += fmt.Sprintf("*%d\r\n", len(entry.store))
			for _, kv := range entry.store {
				response += encodeBulkString(kv)
			}
		}
	}

	if isWrite {
		srv.propagateToReplicas(cmd)
	}

	return
}
