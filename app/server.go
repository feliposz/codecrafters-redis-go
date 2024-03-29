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

	conn, err := listener.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
	defer conn.Close()
	fmt.Println("Client connected: ", conn.RemoteAddr().String())

	for {
		reader := bufio.NewReader(conn)
		buffer := make([]byte, 1024)
		bytesReceived, err := reader.Read(buffer)
		if err == io.EOF || bytesReceived == 0 {
			break
		}
		fmt.Print("Bytes received: ", bytesReceived)
		fmt.Printf(" -> %q\n", buffer[:bytesReceived])

		bytesSent, err := conn.Write([]byte("+PONG\r\n"))
		if err != nil {
			fmt.Println("Error writing response: ", err.Error())
			break
		}
		fmt.Println("Bytes sent: ", bytesSent)
	}
}
