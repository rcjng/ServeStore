package servestore

import (
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
)

var rpcClient RPCClient
var files map[string][]*Block

var localIndex map[string]*FileMetaData
var remoteIndex map[string]*FileMetaData
var remoteBlockStoreAddr string
var syncedLocalIndex map[string]*FileMetaData

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	rpcClient = client

	// Clear global file maps
	files = make(map[string][]*Block)
	syncedLocalIndex = make(map[string]*FileMetaData) // Store synced local index file metadata

	localIndex = getLocalIndex(rpcClient.BaseDir)    // Get local FileMetaInfo map from local index file (index.txt)
	remoteIndex = getRemoteIndex()                   // Get remote FileMetaInfo map from server
	remoteBlockStoreAddr = getRemoteBlockStoreAddr() // Get remote BlockStore address

	handleFiles(rpcClient.BaseDir) // Scan all files in client's base directory

	// Update local index with synced local index
	WriteMetaFile(syncedLocalIndex, rpcClient.BaseDir)
}

func getLocalIndex(directory string) map[string]*FileMetaData {
	log.Println("Retrieving local index...")

	// Create local index file if it does not exist.
	_, err := os.Stat(ConcatPath(directory, DEFAULT_META_FILENAME))
	if err != nil {
		if os.IsNotExist(err) {
			log.Println("Local index does not exist, creating local index...")
			// Create local index file
			index, err := os.Create(ConcatPath(directory, DEFAULT_META_FILENAME))
			if err != nil {
				log.Fatalf("File open error: %v", err)
			}
			index.Close()
		} else {
			log.Fatalf("Stat error: %v", err)
		}
	}

	localIndex, err := LoadMetaFromMetaFile(directory)
	if err != nil {
		log.Fatalf("LoadMetaFromMetaFile error: %v", err)
	}

	// Log localIndex FileMetaData map
	log.Println("Local index file metadata:")
	for _, fileMetaData := range localIndex {
		log.Println(FileMetaDataToString(fileMetaData))
	}

	return localIndex
}

func getRemoteIndex() map[string]*FileMetaData {
	log.Println("Retrieving remote index...")

	remoteIndex := make(map[string]*FileMetaData)
	err := rpcClient.GetFileInfoMap(&remoteIndex)
	if err != nil {
		log.Fatalf("GetFileInfoMap error: %v", err)
	}

	// Log remoteIndex FileMetaData map
	log.Println("Remote index file metadata:")
	for _, fileMetaData := range remoteIndex {
		log.Println(FileMetaDataToString(fileMetaData))
	}

	return remoteIndex
}

func getRemoteBlockStoreAddr() string {
	log.Println("Retrieving remote BlockStore address...")

	var remoteBlockStoreAddr string
	err := rpcClient.GetBlockStoreAddr(&remoteBlockStoreAddr)
	if err != nil {
		log.Fatalf("GetBlockStoreAddr error: %v", err)
	}

	return remoteBlockStoreAddr
}

func handleFiles(directory string) {
	log.Println("Scanning files in directory:", directory)

	err := filepath.WalkDir(directory, syncFile)
	if err != nil {
		log.Fatalf("WalkDir error: %v", err)
	}

	// Handle files in local index that were not found locally
	for filename := range localIndex {
		if remoteFileMetaData, exists := remoteIndex[filename]; exists { // File in remoteIndex, check if already deleted
			// If file has not been deleted in remoteIndex
			if len(remoteFileMetaData.GetBlockHashList()) == 0 ||
				remoteFileMetaData.GetBlockHashList()[0] != TOMBSTONE_HASH ||
				len(remoteFileMetaData.GetBlockHashList()) != 1 {
				// Attempt to update remote file with deletion
				fileMetaData := &FileMetaData{Filename: filename, Version: localIndex[filename].GetVersion() + 1, BlockHashList: []string{TOMBSTONE_HASH}}
				var latestVersion int32
				err := rpcClient.UpdateFile(fileMetaData, &latestVersion)
				if err != nil {
					log.Fatalf("UpdateFile error: %v", err)
				}

				if latestVersion != -1 { // If successful, add file to synced local index
					log.Println(filename, "successfully deleted!")

					syncedLocalIndex[filename] = fileMetaData
				} else { // If unsuccessful, download remote file blocks, overwrite local file, and add file to synced local index
					log.Println(filename, "unsuccessfully deleted, downloading updates!")

					blocks := downloadBlocks(filename)

					updateLocalFile(rpcClient.BaseDir, filename, blocks)

					syncedLocalIndex[filename] = remoteIndex[filename]
				}

				// If file has been deleted in remoteIndex
			} else {
				log.Println("Downloading potential updates for", filename)

				blocks := downloadBlocks(filename)

				updateLocalFile(rpcClient.BaseDir, filename, blocks)

				syncedLocalIndex[filename] = remoteIndex[filename]
			}
		} else {
			// File in localIndex but not in remoteIndex, which is never possible in intended operation: case is explicitly "handled" for clarity and thoroughness
		}

		delete(localIndex, filename)
		delete(remoteIndex, filename)
	}

	// Download files from remoteIndex that are neither scanned nor in localIndex
	for filename, remoteFileMetaData := range remoteIndex {
		log.Println("Downloading updates for", filename)

		blocks := downloadBlocks(filename)

		updateLocalFile(rpcClient.BaseDir, filename, blocks)

		syncedLocalIndex[filename] = remoteFileMetaData
	}
}

func syncFile(path string, d fs.DirEntry, err error) error {
	// If dir entry is a directory, skip (`baseDir` should only have files, not directories)
	if d.IsDir() || d.Name() == DEFAULT_META_FILENAME {
		log.Println("Skipping:", path)
		return nil
	}

	// If there is an error, return the error
	if err != nil {
		log.Printf("syncFile error: %v", err)
		return err
	}

	// Begin algorithm to scan each file and compute hashes.
	log.Println("Syncing file:", path)

	file, err := os.Open(path)
	if err != nil {
		log.Printf("File open error: %v", err)
		return err
	}
	defer file.Close()

	filename := d.Name()
	buf := make([]byte, rpcClient.BlockSize)

	blocks := make([]*Block, 0)
	hashes := make([]string, 0)
	// For each block in the file, calculate the hash and block size and add to files list
	for {
		blockSize, err := file.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Printf("File read error: %v", err)
				return err
			}
		}

		blockData := make([]byte, blockSize)
		copy(blockData, buf[:blockSize])

		block := &Block{BlockData: blockData, BlockSize: int32(blockSize)}
		hash := GetBlockHashString(blockData)

		blocks = append(blocks, block)
		hashes = append(hashes, hash)
	}

	files[filename] = blocks

	// File is in localIndex
	if localFileMetaData, exists := localIndex[filename]; exists {
		localHashes := localFileMetaData.GetBlockHashList()

		isModified := false
		if len(hashes) != len(localHashes) {
			isModified = true
		} else {
			for i := 0; i < len(hashes); i++ {
				if hashes[i] != localHashes[i] {
					isModified = true
					break
				}
			}
		}

		if isModified {
			// Attempt to update remote file with modifications
			uploadBlocks(filename, hashes) // Upload blocks before updating remoteIndex

			fileMetaData := &FileMetaData{Filename: filename, Version: localIndex[filename].GetVersion() + 1, BlockHashList: hashes}
			var latestVersion int32
			err := rpcClient.UpdateFile(fileMetaData, &latestVersion)
			if err != nil {
				log.Fatalf("UpdateFile error: %v", err)
			}

			if latestVersion != -1 { // If successful, upload new file blocks and add file to synced local index
				log.Println(filename, "successfully modified!")

				syncedLocalIndex[filename] = fileMetaData
			} else { // If unsuccessful, download remote file blocks, overwrite local file, and add file to synced local index
				log.Println(filename, "unsuccessfully modified, downloading updates!")

				blocks := downloadBlocks(filename)

				updateLocalFile(rpcClient.BaseDir, filename, blocks)

				syncedLocalIndex[filename] = remoteIndex[filename]
			}
		} else {
			log.Println("Downloading potential updates for", filename)

			blocks := downloadBlocks(filename)

			updateLocalFile(rpcClient.BaseDir, filename, blocks)

			syncedLocalIndex[filename] = remoteIndex[filename]
		}
		// File is not in local index
	} else {
		// Attempt to update remote file with addition
		uploadBlocks(filename, hashes) // Upload blocks before updating remoteIndex

		fileMetaData := &FileMetaData{Filename: filename, Version: 1, BlockHashList: hashes}
		var latestVersion int32
		err := rpcClient.UpdateFile(fileMetaData, &latestVersion)
		if err != nil {
			log.Fatalf("UpdateFile error: %v", err)
		}

		if latestVersion != -1 { // If successful, upload new file blocks and add file to synced local index
			log.Println(filename, "successfully added!")

			syncedLocalIndex[filename] = fileMetaData
		} else { // If unsuccesful, download remote file blocks, overwrite local file, and add file to synced local index
			log.Println(filename, "unsuccessfully added, downloading updates!")

			blocks := downloadBlocks(filename)

			updateLocalFile(rpcClient.BaseDir, filename, blocks)

			syncedLocalIndex[filename] = remoteIndex[filename]
		}
	}

	delete(localIndex, filename)
	delete(remoteIndex, filename)

	return nil
}

func uploadBlocks(filename string, blockHashes []string) {
	log.Println("Uploading blocks for", filename, "with block hashes:", blockHashes)

	// Check BlockStore server for already uploaded blocks
	commonBlocks := []string{}
	err := rpcClient.HasBlocks(blockHashes, remoteBlockStoreAddr, &commonBlocks)
	if err != nil {
		log.Fatalf("HasBlocks error: %v", err)
	}

	log.Println("Common blocks:", commonBlocks)

	// Create a map of blocks already present in the BlockStore server
	presentBlocks := make(map[string]bool)
	for _, blockHash := range commonBlocks {
		presentBlocks[blockHash] = true
	}

	// Upload blocks not already present in the BlockStore server
	for _, block := range files[filename] {
		if _, exists := presentBlocks[GetBlockHashString(block.GetBlockData())]; !exists {
			log.Println("Block", block, "not already present in BlockStore, uploading...")

			var success bool
			err := rpcClient.PutBlock(block, remoteBlockStoreAddr, &success)
			if err != nil || !success {
				log.Fatalf("PutBlock error: %v", err)
			}
		}
	}
}

func downloadBlocks(filename string) []*Block {
	log.Println("Downloading blocks for", filename)

	// If file has been deleted remotely, return special Block list
	if len(remoteIndex[filename].GetBlockHashList()) == 1 && remoteIndex[filename].GetBlockHashList()[0] == TOMBSTONE_HASH {
		log.Println(filename, "has been deleted, no download necessary!")

		block := &Block{BlockData: nil, BlockSize: int32(rpcClient.BlockSize)}
		return []*Block{block}
	}

	blocks := make([]*Block, 0)
	for _, hash := range remoteIndex[filename].GetBlockHashList() {
		block := &Block{}
		err := rpcClient.GetBlock(hash, remoteBlockStoreAddr, block)
		if err != nil {
			log.Fatalf("GetBlock error: %v", err)
		}

		blocks = append(blocks, block)
	}

	return blocks
}

func updateLocalFile(directory string, filename string, blocks []*Block) {
	log.Println("Updating", filename, "in", directory, "with new blocks:", blocks)
	// If file has been deleted remotely, delete local file
	if len(blocks) == 1 && blocks[0].GetBlockData() == nil && blocks[0].BlockSize == int32(rpcClient.BlockSize) {
		log.Println(filename, "has been deleted, removing local file if present!")

		err := os.Remove(ConcatPath(directory, filename))
		if err != nil && !os.IsNotExist(err) {
			log.Fatalf("File remove error: %v", err)
		}
		// Otherwise, overwrite local file
	} else {
		file, err := os.Create(ConcatPath(directory, filename))
		if err != nil {
			log.Fatalf("Create error: %v", err)
		}
		defer file.Close()

		for _, block := range blocks {
			_, err = file.Write(block.GetBlockData())
			if err != nil {
				log.Fatalf("Write blocks error: %v", err)
			}
		}
	}
}
