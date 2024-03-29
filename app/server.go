package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
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
		reader := bufio.NewReader(conn)
		buffer := make([]byte, 1024)
		bytesReceived, err := reader.Read(buffer)
		if err == io.EOF || bytesReceived == 0 {
			break
		}
		fmt.Printf("[#%d] Bytes received: %d -> %q\n", id, bytesReceived, buffer[:bytesReceived])

		bytesSent, err := conn.Write([]byte("+PONG\r\n"))
		if err != nil {
			fmt.Printf("[#%d] Error writing response: %v\n", id, err.Error())
			break
		}
		fmt.Printf("[#%d] Bytes sent: %d\n", id, bytesSent)
	}

	fmt.Printf("[#%d] Client closing\n", id)
}
