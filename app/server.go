package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
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

type listEntry struct {
	data    []string
	blocked []*chan bool
}

// TODO: add some mutexes around these...

type serverState struct {
	streams       map[string]*stream
	store         map[string]string
	ttl           map[string]time.Time
	lists         map[string]*listEntry
	config        serverConfig
	replicas      []replica
	replicaOffset int
	ackReceived   chan bool
}

type clientState struct {
	server *serverState
	id     int
	conn   net.Conn
	multi  bool
	queue  [][]string
	subs   map[string]bool
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

func (srv *serverState) getList(key string, create bool) (list *listEntry) {
	list, found := srv.lists[key]
	if !found && create {
		list = new(listEntry)
		srv.lists[key] = list
	}
	return list
}

func newServer(config serverConfig) *serverState {
	var srv serverState
	srv.store = make(map[string]string)
	srv.lists = make(map[string]*listEntry)
	srv.ttl = make(map[string]time.Time)
	srv.streams = make(map[string]*stream)
	srv.ackReceived = make(chan bool)
	srv.config = config
	return &srv
}

func newClient(server *serverState, id int, conn net.Conn) *clientState {
	var client clientState
	client.server = server
	client.id = id
	client.conn = conn
	client.subs = make(map[string]bool)
	return &client
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
		client := newClient(srv, id, conn)
		go client.serve()
	}
}

func (cli *clientState) serve() {
	fmt.Printf("[#%d] Client connected: %v\n", cli.id, cli.conn.RemoteAddr().String())

	//scanner := bufio.NewScanner(conn)
	reader := bufio.NewReader(cli.conn)

	for {
		cmd, _, err := decodeStringArray(reader)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("[%d] Error decoding command: %v\n", cli.id, err.Error())
		}

		if len(cmd) == 0 {
			break
		}

		var response string
		var resynch bool

		if cmd[0] == "EXEC" {
			if cli.multi {
				responses := []any{}
				for _, cmd := range cli.queue {
					response, _ := cli.server.handleCommand(cmd, cli)
					responses = append(responses, response)
				}
				response = encodeArray(responses)
				cli.queue = nil
				cli.multi = false
			} else {
				response = encodeError(fmt.Errorf("EXEC without MULTI"))
			}
		} else if cmd[0] == "DISCARD" {
			if cli.multi {
				response = encodeSimpleString("OK")
				cli.queue = nil
				cli.multi = false
			} else {
				response = encodeError(fmt.Errorf("DISCARD without MULTI"))
			}
		} else if cli.multi {
			cli.queue = append(cli.queue, cmd)
			response := encodeSimpleString("QUEUED")
			bytesSent, err := cli.conn.Write([]byte(response))
			if err != nil {
				fmt.Printf("[#%d] Error writing response: %v\n", cli.id, err.Error())
				break
			}
			fmt.Printf("[#%d] Bytes sent: %d %q\n", cli.id, bytesSent, response)
			continue
		} else {
			fmt.Printf("[#%d] Command = %q\n", cli.id, cmd)
			response, resynch = cli.server.handleCommand(cmd, cli)
		}

		if len(response) > 0 {
			bytesSent, err := cli.conn.Write([]byte(response))
			if err != nil {
				fmt.Printf("[#%d] Error writing response: %v\n", cli.id, err.Error())
				break
			}
			fmt.Printf("[#%d] Bytes sent: %d %q\n", cli.id, bytesSent, response)
		}

		if resynch {
			size := sendFullResynch(cli.conn)
			fmt.Printf("[#%d] full resynch sent: %d\n", cli.id, size)
			cli.server.replicas = append(cli.server.replicas, replica{cli.conn, 0, 0})
			fmt.Printf("[#%d] Client promoted to replica\n", cli.id)
			return
		}
	}

	fmt.Printf("[#%d] Client closing\n", cli.id)
	cli.conn.Close()
}

var subscribeModeCommands = []string{
	"SUBSCRIBE",
	"UNSUBSCRIBE",
	"PSUBSCRIBE",
	"PUNSUBSCRIBE",
	"PING",
	"QUIT",
}

func (srv *serverState) handleCommand(cmd []string, cli *clientState) (response string, resynch bool) {
	isWrite := false
	command := strings.ToUpper(cmd[0])

	subscribeMode := len(cli.subs) > 0
	isSubscribeModeCommand := slices.Index(subscribeModeCommands, command) != -1

	if subscribeMode && !isSubscribeModeCommand {
		response = encodeError(fmt.Errorf("can't execute '%s'", command))
		return
	}

	switch command {
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

	case "MULTI":
		cli.multi = true
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

	case "RPUSH":
		listKey, values := cmd[1], cmd[2:]
		list := srv.getList(listKey, true)
		list.data = append(list.data, values...)
		response = encodeInt(len(list.data))
		if len(list.blocked) > 0 {
			waitingData := list.blocked[0]
			*waitingData <- true
		}

	case "LPUSH":
		listKey, values := cmd[1], cmd[2:]
		list := srv.getList(listKey, true)
		slices.Reverse(values)
		list.data = append(values, list.data...)
		response = encodeInt(len(list.data))
		if len(list.blocked) > 0 {
			waitingData := list.blocked[0]
			*waitingData <- true
		}

	case "LRANGE":
		listKey := cmd[1]
		start, _ := strconv.Atoi(cmd[2])
		end, _ := strconv.Atoi(cmd[3])
		response = encodeStringArray(nil)
		list := srv.getList(listKey, false)
		if list != nil {
			if start < 0 {
				start += len(list.data)
				if start < 0 {
					start = 0
				}
			}
			if end < 0 {
				end += len(list.data)
				if end < 0 {
					end = 0
				}
			}
			if start < len(list.data) && start <= end {
				if end >= len(list.data) {
					end = len(list.data) - 1
				}
				response = encodeStringArray(list.data[start : end+1])
			}
		}

	case "LLEN":
		listKey := cmd[1]
		list := srv.getList(listKey, false)
		if list != nil {
			response = encodeInt(len(list.data))
		} else {
			response = encodeInt(0)
		}

	case "LPOP":
		listKey := cmd[1]
		list := srv.getList(listKey, false)
		if list != nil && len(list.data) >= 1 {
			if len(cmd) < 3 {
				response = encodeBulkString(list.data[0])
				list.data = slices.Delete(list.data, 0, 1)
			} else {
				count, _ := strconv.Atoi(cmd[2])
				if count > len(list.data) {
					count = len(list.data)
				}
				response = encodeStringArray(list.data[:count])
				list.data = slices.Delete(list.data, 0, count)
			}
		} else {
			response = encodeBulkString("")
		}

	case "BLPOP":
		listKey := cmd[1]
		blockTimeout, _ := strconv.ParseFloat(cmd[2], 64)
		list := srv.getList(listKey, true)
		timedOut := false
		if len(list.data) < 1 {
			waitingData := make(chan bool)
			list.blocked = append(list.blocked, &waitingData)
			if blockTimeout > 0 {
				blockTimeout *= 1000
				fmt.Printf("Waiting for a push on list %s (timeout = %f ms)...\n", listKey, blockTimeout)
				timer := time.After(time.Duration(blockTimeout) * time.Millisecond)
				select {
				case <-waitingData:
					timedOut = false
				case <-timer:
					timedOut = true
				}
			} else {
				fmt.Printf("Waiting for a write on stream %s (no timeout!)...\n", listKey)
				<-waitingData
			}
			list.blocked = slices.DeleteFunc(list.blocked, func(ch *chan bool) bool { return ch == &waitingData })
		}
		if timedOut {
			response = encodeBulkString("")
		} else {
			value := list.data[0]
			response = encodeStringArray([]string{listKey, value})
			list.data = slices.Delete(list.data, 0, 1)
		}

	case "SUBSCRIBE":
		channel := cmd[1]
		cli.subs[channel] = true
		response = encodeArray([]any{"subscribe", channel, len(cli.subs)})

	}

	if isWrite {
		srv.propagateToReplicas(cmd)
	}

	return
}
