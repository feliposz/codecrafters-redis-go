package main

import (
	"fmt"
	"os"
	"path/filepath"
)

func (srv *serverState) aofInit() {
	aofPath := filepath.Join(srv.config.dir, srv.config.appendDirName)
	if err := os.MkdirAll(aofPath, 0750); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create AOF directory '%s': %v\n", aofPath, err)
		os.Exit(1)
	}

	aofFileName := srv.config.appendFileName + ".1.incr.aof"
	aofFilePath := filepath.Join(srv.config.dir, srv.config.appendDirName, aofFileName)
	aofFile, err := os.OpenFile(aofFilePath, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create AOF file '%s': %v\n", aofFilePath, err)
		os.Exit(1)
	}
	srv.aofFile = aofFile

	manifestFileName := filepath.Join(srv.config.dir, srv.config.appendDirName, srv.config.appendFileName+".manifest")
	manifestFile, err := os.OpenFile(manifestFileName, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create manifest file '%s': %v\n", manifestFileName, err)
		os.Exit(1)
	}
	fmt.Fprintf(manifestFile, "file %s seq %d type %s\n", aofFileName, 1, "i")
	manifestFile.Close()
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
