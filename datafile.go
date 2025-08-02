package main

import (
	"fmt"
	"os"
	"sync"
	"time"
)

var FILE_PATH = "datafile.log"

type KV map[string]Meta

var (
	kvInstance KV
	once       sync.Once
)

func GetKVInstance() KV {
	once.Do(func() {
		kvInstance = make(KV)
	})
	return kvInstance
}

type Meta struct {
	TimeStamp   time.Time
	FileId      int
	StartOffset int
	Size        int
}
type DataFile struct {
	FileID     int
	mutex      sync.RWMutex
	writer     *os.File
	reader     *os.File
	lastoffset int64
}

func NewDataFile() *DataFile {
	datafile := &DataFile{
		FileID:     HashString(FILE_PATH),
		mutex:      sync.RWMutex{},
		lastoffset: 0,
	}

	writer, err := os.OpenFile(FILE_PATH, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	datafile.writer = writer

	reader, err := os.Open("datafile.log")
	if err != nil {
		panic(err)
	}
	datafile.reader = reader

	return datafile
}

func (d *DataFile) put(key string, val []byte) {
	record := NewRecord(d.FileID, key, val)
	//Insert record using writer
	d.mutex.Lock()
	n, err := d.writer.Write(record)
	if err != nil {
		fmt.Println("error writing %v", err)
	}
	d.mutex.Unlock()
	d.lastoffset = d.lastoffset + int64(n)
	//get the metaData from the inserted record
	//insert this metadata into Meta DS and insert to kv instance

}
