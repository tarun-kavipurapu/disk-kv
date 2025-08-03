package main

import (
	"fmt"
	"os"
	"sync"
)

var FILE_PATH = "datafile.log"

type KV map[string]*Meta

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
	TimeStamp   uint32
	FileId      int
	StartOffset int64
	Size        int //size of the entire record buffer we are going to read
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
		FileID: HashString(FILE_PATH),
		mutex:  sync.RWMutex{},
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

	fileInfo, err := datafile.writer.Stat()
	datafile.lastoffset = fileInfo.Size()

	return datafile
}

func (d *DataFile) put(key string, val []byte) {
	recordBytes, record := NewRecord(d.FileID, key, val)
	//Insert record using writer
	d.mutex.Lock()
	n, err := d.writer.Write(recordBytes)
	if err != nil {
		fmt.Println("error writing %v", err)
	}
	d.mutex.Unlock()
	// so basically from metadata lets make a read of startoffset+size read into buffer
	metaData := &Meta{
		TimeStamp:   record.Header.Timestamp,
		FileId:      d.FileID,
		StartOffset: d.lastoffset,
		Size:        n,
	}
	kv := GetKVInstance()
	kv[key] = metaData
	d.lastoffset = d.lastoffset + int64(n)

	//get the metaData from the inserted record
	//insert this metadata into Meta DS and insert to kv instance

}

func (f *DataFile) read(key string) []byte {
	// get metadata first
	kv := GetKVInstance()
	meta := kv[key]
	fmt.Println(meta.StartOffset)
	startOffset := meta.StartOffset
	size := meta.Size
	buffer := make([]byte, size)
	f.mutex.Lock()
	defer f.mutex.Unlock()

	_, err := f.reader.Seek(startOffset, 0) // 0 means relative to the start of the file
	if err != nil {
		fmt.Printf("Error seeking to offset %d: %v\n", startOffset, err)
		return nil
	}

	_, err = f.reader.Read(buffer)
	if err != nil {
		fmt.Printf("Error reading data: %v\n", err)
		return nil
	}

	record, err := DecodeRecord(buffer)
	if err != nil {
		fmt.Printf("Error decoding record: %v\n", err)
		return nil
	}
	return record.value
}
