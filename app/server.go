package main

import (
	"bufio"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
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

type stream struct {
	first   [2]uint64
	last    [2]uint64
	entries []*streamEntry
	blocked []*chan bool
}

type streamEntry struct {
	id    [2]uint64
	store []string
}

type replica struct {
	conn      net.Conn
	offset    int
	ackOffset int // TODO: keep track of this also
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

func (srv *serverState) replicaHandshake() {
	masterConn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", srv.config.replicaofHost, srv.config.replicaofPort))
	if err != nil {
		fmt.Printf("Failed to connect to master %v\n", err)
		os.Exit(1)
	}

	// TODO: check responses
	reader := bufio.NewReader(masterConn)
	masterConn.Write([]byte(encodeStringArray([]string{"PING"})))
	reader.ReadString('\n')
	masterConn.Write([]byte(encodeStringArray([]string{"REPLCONF", "listening-port", strconv.Itoa(srv.config.port)})))
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

	go srv.handlePropagation(reader, masterConn)
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
			return
		}
	}

	fmt.Printf("[#%d] Client closing\n", id)
	conn.Close()
}

func encodeBulkString(s string) string {
	if len(s) == 0 {
		return "$-1\r\n"
	}
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

func encodeSimpleString(s string) string {
	return fmt.Sprintf("+%s\r\n", s)
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

func encodeError(e error) string {
	// TODO: handle error types...
	return fmt.Sprintf("-ERR %s\r\n", e.Error())
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
			startMs, startSeq, startHasSeq, _ := stream.splitId(start)
			if !startHasSeq {
				startSeq = 0
			}
			startIndex = binarySearchEntries(stream.entries, startMs, startSeq, 0, len(stream.entries)-1)
		}

		if end == "+" {
			endIndex = len(stream.entries) - 1
		} else {
			endMs, endSeq, endHasSeq, _ := stream.splitId(end)
			if !endHasSeq {
				endSeq = math.MaxUint64
			}
			endIndex = binarySearchEntries(stream.entries, endMs, endSeq, startIndex, len(stream.entries)-1)
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
				startMs, startSeq, startHasSeq, _ = stream.splitId(start)
				if !startHasSeq {
					startSeq = 0
				}
			}

			var entry *streamEntry
			var startIndex int

			for entry == nil {
				startIndex = binarySearchEntries(stream.entries, startMs, startSeq, startIndex, len(stream.entries)-1)

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
		propagate(srv.replicas, cmd)
	}

	return
}

func newStream() *stream {
	return &stream{
		first:   [2]uint64{0, 0},
		last:    [2]uint64{0, 0},
		entries: make([]*streamEntry, 0),
		blocked: make([]*chan bool, 0),
	}
}

func (s *stream) splitId(id string) (millisecondsTime, sequenceNumber uint64, hasSequence bool, err error) {
	parts := strings.Split(id, "-")
	millisecondsTime, err = strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return
	}
	if len(parts) > 1 {
		sequenceNumber, err = strconv.ParseUint(parts[1], 10, 64)
		hasSequence = true
	}
	return
}

func (s *stream) getNext(id string) (millisecondsTime, sequenceNumber uint64, err error) {
	parts := strings.Split(id, "-")

	if len(parts) == 1 && parts[0] == "*" {
		millisecondsTime = uint64(time.Now().UnixMilli())
		if millisecondsTime == s.last[0] {
			sequenceNumber = s.last[1] + 1
		}
	} else if len(parts) == 2 && parts[1] == "*" {
		millisecondsTime, _ = strconv.ParseUint(parts[0], 10, 64)
		if millisecondsTime == s.last[0] {
			sequenceNumber = s.last[1] + 1
		} else if millisecondsTime > s.last[0] {
			sequenceNumber = 0
		} else {
			return 0, 0, fmt.Errorf("The ID specified in XADD is equal or smaller than the target stream top item")
		}
	} else {
		millisecondsTime, _ = strconv.ParseUint(parts[0], 10, 64)
		sequenceNumber, _ = strconv.ParseUint(parts[1], 10, 64)
	}

	if millisecondsTime == 0 && sequenceNumber == 0 {
		return 0, 0, fmt.Errorf("The ID specified in XADD must be greater than 0-0")
	}

	if millisecondsTime < s.last[0] || millisecondsTime == s.last[0] && sequenceNumber <= s.last[1] {
		return 0, 0, fmt.Errorf("The ID specified in XADD is equal or smaller than the target stream top item")
	}

	return
}

func (s *stream) addStreamEntry(id string) (*streamEntry, error) {
	millisecondsTime, sequenceNumber, err := s.getNext(id)
	if err != nil {
		return nil, err
	}

	if s.first[0] == 0 && s.first[1] == 0 {
		s.first[0], s.first[1] = millisecondsTime, sequenceNumber
	}
	s.last[0], s.last[1] = millisecondsTime, sequenceNumber

	entry := new(streamEntry)
	entry.id[0] = millisecondsTime
	entry.id[1] = sequenceNumber
	entry.store = make([]string, 0)
	s.entries = append(s.entries, entry)
	return entry, nil
}

func binarySearchEntries(entries []*streamEntry, targetMs, targetSeq uint64, lo, hi int) int {
	for lo <= hi {
		mid := (lo + hi) / 2
		entry := entries[mid]
		if targetMs == entry.id[0] && targetSeq == entry.id[1] {
			lo = mid
			break
		} else if targetMs == entry.id[0] && entry.id[1] > targetSeq {
			hi = mid - 1
		} else if targetMs == entry.id[0] && entry.id[1] < targetSeq {
			lo = mid + 1
		} else if targetMs < entry.id[0] {
			hi = mid - 1
		} else {
			lo = mid + 1
		}
	}
	return lo
}

func propagate(replicas []replica, cmd []string) {
	if len(replicas) == 0 {
		return
	}
	fmt.Printf("Propagating = %q\n", cmd)
	for i := 0; i < len(replicas); i++ {
		fmt.Printf("Replicating to: %s\n", replicas[i].conn.RemoteAddr().String())
		bytesWritten, err := replicas[i].conn.Write([]byte(encodeStringArray(cmd)))
		// remove stale replicas
		if err != nil {
			fmt.Printf("Disconnected: %s\n", replicas[i].conn.RemoteAddr().String())
			if len(replicas) > 0 {
				last := len(replicas) - 1
				replicas[i] = replicas[last]
				replicas = replicas[:last]
				i--
			}
		}
		replicas[i].offset += bytesWritten
	}
}

func (srv *serverState) handleWait(count, timeout int) string {
	getAckCmd := []byte(encodeStringArray([]string{"REPLCONF", "GETACK", "*"}))

	acks := 0

	for i := 0; i < len(srv.replicas); i++ {
		if srv.replicas[i].offset > 0 {
			bytesWritten, _ := srv.replicas[i].conn.Write(getAckCmd)
			srv.replicas[i].offset += bytesWritten
			go func(conn net.Conn) {
				fmt.Println("waiting response from replica", conn.RemoteAddr().String())
				buffer := make([]byte, 1024)
				// TODO: Ignoring result, just "flushing" the response
				_, err := conn.Read(buffer)
				if err == nil {
					fmt.Println("got response from replica", conn.RemoteAddr().String())
				} else {
					fmt.Println("error from replica", conn.RemoteAddr().String(), " => ", err.Error())
				}
				srv.ackReceived <- true
			}(srv.replicas[i].conn)
		} else {
			acks++
		}
	}

	timer := time.After(time.Duration(timeout) * time.Millisecond)

outer:
	for acks < count {
		select {
		case <-srv.ackReceived:
			acks++
			fmt.Println("acks =", acks)
		case <-timer:
			fmt.Println("timeout! acks =", acks)
			break outer
		}
	}

	return encodeInt(acks)
}

func (srv *serverState) handlePropagation(reader *bufio.Reader, masterConn net.Conn) {
	defer masterConn.Close()

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
		response, _ := srv.handleCommand(cmd)
		//fmt.Printf("response = %q\n", response)
		if strings.ToUpper(cmd[0]) == "REPLCONF" {
			//fmt.Printf("ack = %q\n", cmd)
			_, err := masterConn.Write([]byte(response))
			if err != nil {
				fmt.Printf("Error responding to master: %v\n", err.Error())
				break
			}
		}
		srv.replicaOffset += cmdSize
	}
}

func readEncodedInt(reader *bufio.Reader) (int, error) {
	mask := byte(0b11000000)
	b0, err := reader.ReadByte()
	if err != nil {
		return 0, err
	}
	if b0&mask == 0b00000000 {
		return int(b0), nil
	} else if b0&mask == 0b01000000 {
		b1, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}
		return int(b1)<<6 | int(b0&mask), nil
	} else if b0&mask == 0b10000000 {
		b1, _ := reader.ReadByte()
		b2, _ := reader.ReadByte()
		b3, _ := reader.ReadByte()
		b4, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}
		// TODO: check endianness!
		return int(b1)<<24 | int(b2)<<16 | int(b3)<<8 | int(b4), nil
	} else if b0 >= 0b11000000 && b0 <= 0b11000010 { // Special format: Integers as String
		var b1, b2, b3, b4 byte
		b1, err = reader.ReadByte()
		if b0 >= 0b11000001 {
			b2, err = reader.ReadByte()
		}
		if b0 == 0b11000010 {
			b3, _ = reader.ReadByte()
			b4, err = reader.ReadByte()
		}
		if err != nil {
			return 0, err
		}
		return int(b1) | int(b2)<<8 | int(b3)<<16 | int(b4)<<24, nil
	} else {
		return 0, errors.New("not implemented")
	}
}

func readEncodedString(reader *bufio.Reader) (string, error) {
	size, err := readEncodedInt(reader)
	if err != nil {
		return "", err
	}
	data := make([]byte, size)
	actual, err := reader.Read(data)
	if err != nil {
		return "", err
	}
	if int(size) != actual {
		return "", errors.New("unexpected string length")
	}
	return string(data), nil
}

func readRDB(rdbPath string, store map[string]string, ttl map[string]time.Time) error {
	file, err := os.Open(rdbPath)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	header := make([]byte, 9)
	reader.Read(header)
	if slices.Compare(header[:5], []byte("REDIS")) != 0 {
		return errors.New("not a RDB file")
	}

	version, _ := strconv.Atoi(string(header[5:]))
	fmt.Printf("File version: %d\n", version)

	for eof := false; !eof; {

		startDataRead := false
		opCode, err := reader.ReadByte()

		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// TODO: handle errors properly
		switch opCode {
		case 0xFA: // Auxiliary fields
			key, _ := readEncodedString(reader)
			switch key {
			case "redis-ver":
				value, _ := readEncodedString(reader)
				fmt.Printf("Aux: %s = %v\n", key, value)
			case "redis-bits":
				bits, _ := readEncodedInt(reader)
				fmt.Printf("Aux: %s = %v\n", key, bits)
			case "ctime":
				ctime, _ := readEncodedInt(reader)
				fmt.Printf("Aux: %s = %v (%v)\n", key, ctime, time.Unix(int64(ctime), 0))
			case "used-mem":
				usedmem, _ := readEncodedInt(reader)
				fmt.Printf("Aux: %s = %v\n", key, usedmem)
			case "aof-preamble":
				size, _ := readEncodedInt(reader)
				// preamble := make([]byte, size)
				// reader.Read(preamble)
				fmt.Printf("Aux: %s = %d\n", key, size)
			default:
				return fmt.Errorf("unknown auxiliary field: %q", key)
			}

		case 0xFB: // Hash table sizes for the main keyspace and expires
			keyspace, _ := readEncodedInt(reader)
			expires, _ := readEncodedInt(reader)
			fmt.Printf("Hash table sizes: keyspace = %d, expires = %d\n", keyspace, expires)
			startDataRead = true

		case 0xFE: // Database Selector
			db, _ := readEncodedInt(reader)
			fmt.Printf("Database Selector = %d\n", db)

		case 0xFF: // End of the RDB file
			// TODO: implement CRC?
			eof = true

		default:
			return fmt.Errorf("unknown op code: %x", opCode)
		}

		if startDataRead {
			for {
				valueType, err := reader.ReadByte()
				if err != nil {
					return err
				}

				var expiration time.Time
				if valueType == 0xFD {
					bytes := make([]byte, 4)
					reader.Read(bytes)
					expiration = time.Unix(int64(bytes[0])|int64(bytes[1])<<8|int64(bytes[2])<<16|int64(bytes[3])<<24, 0)
					valueType, err = reader.ReadByte()
				} else if valueType == 0xFC {
					bytes := make([]byte, 8)
					reader.Read(bytes)
					expiration = time.UnixMilli(int64(bytes[0]) | int64(bytes[1])<<8 | int64(bytes[2])<<16 | int64(bytes[3])<<24 |
						int64(bytes[4])<<32 | int64(bytes[5])<<40 | int64(bytes[6])<<48 | int64(bytes[7])<<56)
					valueType, err = reader.ReadByte()
				} else if valueType == 0xFF {
					startDataRead = false
					reader.UnreadByte()
					break
				}

				if err != nil {
					return err
				}

				if valueType != 0 {
					return fmt.Errorf("value type not implemented: %x", valueType)
				}

				key, _ := readEncodedString(reader)
				value, _ := readEncodedString(reader)
				fmt.Printf("Reading key/value: %q => %q Expiration: (%v)\n", key, value, expiration)

				now := time.Now()

				if expiration.IsZero() || expiration.After(now) {
					if expiration.After(now) {
						ttl[key] = expiration
					}
					store[key] = value
				}
			}
		}
	}

	return nil
}
