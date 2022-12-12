package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"rcjng/pkg/servestore"
	"sort"
	"strconv"
	"strings"
)

func main() {

	downServers := flag.String("downServers", "", "Comma-separated list of server IDs that have failed")
	flag.Parse()

	if flag.NArg() != 3 {
		fmt.Printf("Usage: %s numServers blockSize inpFilename\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	numServers, err := strconv.Atoi(flag.Arg(0))
	if err != nil {
		log.Fatal("Invalid number of servers argument: ", flag.Arg(0))
	}

	blockSize, err := strconv.Atoi(flag.Arg(1))
	if err != nil {
		log.Fatal("Invalid block size argument: ", flag.Arg(0))
	}

	inpFilename := flag.Arg(2)

	log.Println("Total number of blockStore servers: ", numServers)
	log.Println("Block size: ", blockSize)
	log.Println("Processing input data filename: ", inpFilename)

	downServersMap := make(map[int]int)

	if *downServers != "" {
		for _, downServer := range strings.Split(*downServers, ",") {
			server, err := strconv.Atoi(downServer)
			if err != nil {
				log.Println("Error converting", downServer, "to int")
			}
			downServersMap[server] = server
		}
	} else {
		log.Println("No servers are in a failed state")
	}

	/**
	 * Steps:
	 * 1. Break `inpFilename` into blocks of size `blockSize`
	 * 2. Compute hash for each block (SHA-256)
	 * 3. Compute hash for each server (blockstore0, blockstore1, ..., blockstore`n-1`)
	 * 3. Use consistent hashing algorithm in lecture to map blocks to server number
	 * 4. Return pairs where first is block hash and second is blockstore server number
	 **/

	blockHashes := getFileBlockHashes(inpFilename, blockSize)
	serverHashes, serverHashesMap := getServerHashes(numServers)

	sort.Strings(serverHashes)
	sort.Strings(blockHashes)
	// fmt.Println("Server Hashes:", serverHashes)
	// fmt.Println("Server Hashes Map:", serverHashesMap)

	pairs := "{"
	for _, blockHash := range blockHashes {
		server := serverHashesMap[serverHashes[0]]

		serverIndex := 0
		for s, serverHash := range serverHashes {
			// fmt.Println("blockHash", blockHash, "serverHash", serverHash)
			if blockHash < serverHash {
				server = serverHashesMap[serverHash]
				serverIndex = s
				// fmt.Println("server", server, "serverIndex", serverIndex)
				break
			}
		}

		for {
			_, exists := downServersMap[server]
			if !exists {
				break
			}

			if serverIndex == len(serverHashes)-1 {
				serverIndex = 0
			} else {
				serverIndex += 1
			}

			server = serverHashesMap[serverHashes[serverIndex]]
		}

		pairs += "{"
		pairs += blockHash
		pairs += ", "
		pairs += strconv.Itoa(server)
		pairs += "}, "
	}
	pairs = pairs[:len(pairs)-2] + "}"

	fmt.Println(pairs)

	// This is an example of the format of the output
	// Your program will emit pairs for each block has where the
	// first part of the pair is the block hash, and the second
	// element is the server number that the block resides on
	//
	// This output is simply to show the format, the actual mapping
	// isn't based on consistent hashing necessarily
	// fmt.Println("{{672e9bff6a0bc59669954be7b2c2726a74163455ca18664cc350030bc7eca71e, 7}, {31f28d5a995dcdb7c5358fcfa8b9c93f2b8e421fb4a268ca5dc01ca4619dfe5f,2}, {172baa036a7e9f8321cb23a1144787ba1a0727b40cb6283dbb5cba20b84efe50,1}, {745378a914d7bcdc26d3229f98fc2c6887e7d882f42d8491530dfaf4effef827,5}, {912b9d7afecb114fdaefecfa24572d052dde4e1ad2360920ebfe55ebf2e1818e,0}}")
}

func getServerHashes(numServers int) ([]string, map[string]int) {
	hashes := make([]string, 0)
	hashesMap := make(map[string]int)
	for i := 0; i < numServers; i++ {
		hash := servestore.GetBlockHashString([]byte("blockserver" + strconv.Itoa(i)))
		hashes = append(hashes, hash)
		hashesMap[hash] = i
	}

	return hashes, hashesMap
}

func getFileBlockHashes(filename string, blockSize int) []string {
	// Begin algorithm to scan each file and compute hashes.s
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("File open error: %v", err)
	}
	defer file.Close()

	buf := make([]byte, blockSize)
	hashes := make([]string, 0)
	// For each block in the file, calculate the hash and block size and add to files list
	for {
		blockSize, err := file.Read(buf)
		if err != nil {
			if err != io.EOF {
				log.Fatalf("File read error: %v", err)
			}

			break
		}

		blockData := buf[:blockSize]
		hashes = append(hashes, servestore.GetBlockHashString(blockData))
	}

	return hashes
}
