package main

import "hash/fnv"

func HashString(filePath string) int {
	h := fnv.New32a() // Create a new FNV-1a hash
	h.Write([]byte(filePath))
	return int(h.Sum32()) // Convert the hash to an int
}
