package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
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
		parts := strings.Split(config.replicaofHost, " ")
		config.replicaofHost = parts[0]
		config.replicaofPort, _ = strconv.Atoi(parts[1])
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

	//scanner := bufio.NewScanner(conn)
	reader := bufio.NewReader(conn)

	for {
		cmd, _, err := decodeStringArray(reader)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("[%d] Error decoding command: %v\n", id, err.Error())
		}

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
			size := sendFullResynch(conn)
			fmt.Printf("[#%d] full resynch sent: %d\n", id, size)
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

	case "PING":
		response = "+PONG\r\n"

	case "ECHO":
		response = encodeBulkString(cmd[1])

	case "INFO":
		if len(cmd) == 2 && strings.ToUpper(cmd[1]) == "REPLICATION" {
			response = encodeBulkString(fmt.Sprintf("role:%s\r\nmaster_replid:%s\r\nmaster_repl_offset:%d",
				srv.config.role, srv.config.replid, srv.config.replOffset))
		}

	case "CONFIG":
		switch cmd[2] {
		case "dir":
			response = encodeStringArray([]string{"dir", srv.config.dir})
		case "dbfilename":
			response = encodeStringArray([]string{"dbfilename", srv.config.dbfilename})
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

	case "INCR":
		isWrite = true
		key := cmd[1]
		if curr, found := srv.store[key]; found {
			value, err := strconv.Atoi(curr)
			if err != nil {
				response = encodeError(fmt.Errorf("value is not an integer or out of range"))
			} else {
				value++
				srv.store[key] = strconv.Itoa(value)
				response = encodeInt(value)
			}
		} else {
			value := 1
			srv.store[key] = strconv.Itoa(value)
			response = encodeInt(value)
		}

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

	case "WAIT":
		count, _ := strconv.Atoi(cmd[1])
		timeout, _ := strconv.Atoi(cmd[2])
		response = srv.handleWait(count, timeout)

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
		streamKey, id := cmd[1], cmd[2]
		response = srv.handleStreamAdd(streamKey, id, cmd[3:])

	case "XRANGE":
		streamKey, start, end := cmd[1], cmd[2], cmd[3]
		response = srv.handleStreamRange(streamKey, start, end)

	case "XREAD":
		response = srv.handleStreamRead(cmd)

	}

	if isWrite {
		srv.propagateToReplicas(cmd)
	}

	return
}
