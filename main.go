package main

import "fmt"

/*
kv functions:
- get(key)return value
- put(key,value)return "Sucess"|failure(with error)
- delete(key) return Sucess|failure(with error)
Since this is a disk based kv the map should be encoded in the disk file

*/

func main() {

	// fmt.Println("This is a disk based kv ")
	dataFile := NewDataFile()
	dataFile.put("Tarun", []byte("Kavi"))
	dataFile.put("dank", []byte("tarun"))
	fmt.Println(string(dataFile.read("Tarun")))

}
