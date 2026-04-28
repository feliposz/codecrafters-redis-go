package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func (srv *serverState) aofInit() {
	aofPath := filepath.Join(srv.config.dir, srv.config.appendDirName)
	if err := os.MkdirAll(aofPath, 0750); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create AOF directory '%s': %v\n", aofPath, err)
		os.Exit(1)
	}

	aofFileName := srv.config.appendFileName + ".1.incr.aof"

	manifestFileName := filepath.Join(srv.config.dir, srv.config.appendDirName, srv.config.appendFileName+".manifest")
	manifestFile, err := os.OpenFile(manifestFileName, os.O_RDONLY, 0640)
	if err == nil {
		scanner := bufio.NewScanner(manifestFile)
		for scanner.Scan() {
			line := scanner.Text()
			parts := strings.Split(line, " ")
			if len(parts) != 6 {
				panic("invalid manifest file format")
			}
			if parts[5] == "i" {
				aofFileName = parts[1]
				break
			}
		}
		manifestFile.Close()
	} else {
		if os.IsNotExist(err) {
			manifestFile, err := os.OpenFile(manifestFileName, os.O_CREATE|os.O_RDWR, 0640)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to create manifest file '%s': %v\n", manifestFileName, err)
				os.Exit(1)
			}
			fmt.Fprintf(manifestFile, "file %s seq %d type %s\n", aofFileName, 1, "i")
			manifestFile.Close()
		} else {
			fmt.Fprintf(os.Stderr, "Failed to read manifest file '%s': %v\n", manifestFileName, err)
			os.Exit(1)
		}
	}

	aofFilePath := filepath.Join(srv.config.dir, srv.config.appendDirName, aofFileName)
	aofFile, err := os.OpenFile(aofFilePath, os.O_RDONLY, 0640)
	if err == nil {
		srv.aofReplay(aofFile)
		aofFile.Close()
		aofFile, err := os.OpenFile(aofFilePath, os.O_APPEND|os.O_WRONLY, 0640)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to append to AOF file '%s': %v\n", aofFilePath, err)
			os.Exit(1)
		}
		srv.aofFile = aofFile
	} else {
		if os.IsNotExist(err) {
			aofFile, err := os.OpenFile(aofFilePath, os.O_CREATE|os.O_RDWR, 0640)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to create AOF file '%s': %v\n", aofFilePath, err)
				os.Exit(1)
			}
			srv.aofFile = aofFile
		} else {
			fmt.Fprintf(os.Stderr, "Failed to open AOF file '%s': %v\n", aofFilePath, err)
			os.Exit(1)
		}
	}

}

func (srv *serverState) aofReplay(aofFile *os.File) {
	reader := bufio.NewReader(aofFile)

	for {
		cmd, _, err := decodeStringArray(reader)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("[AOF] Error decoding command: %v\n", err.Error())
		}

		if len(cmd) == 0 {
			break
		}

		fmt.Printf("[AOF] Command = %q\n", cmd)
		srv.handleCommand(cmd, nil, true)
	}
}

func (srv *serverState) aofPersist(cmd []string) {
	if srv.aofFile == nil {
		return
	}
	if srv.config.appendFsync == "always" {
		cmdBuf := encodeStringArray(cmd)
		fmt.Println("Persisting command: ", cmdBuf)
		srv.aofFile.Write([]byte(cmdBuf))
		srv.aofFile.Sync()
	}
}
