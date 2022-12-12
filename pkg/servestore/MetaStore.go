package servestore

import (
	context "context"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	// If the file exists in MetaStore already, update if possible
	var latestVersion int32 = -1
	if metaStoreFileMetaData, exists := m.FileMetaMap[fileMetaData.GetFilename()]; exists {
		// If `fileMetaData` version is 1 greater than MetaStore version, update MetaStore BlockHashList and Version
		if fileMetaData.GetVersion() == metaStoreFileMetaData.GetVersion()+1 {
			metaStoreFileMetaData.BlockHashList = fileMetaData.GetBlockHashList()
			metaStoreFileMetaData.Version = fileMetaData.GetVersion()
			latestVersion = metaStoreFileMetaData.GetVersion()
		}
	} else {
		m.FileMetaMap[fileMetaData.GetFilename()] = &FileMetaData{Filename: fileMetaData.GetFilename(), Version: fileMetaData.GetVersion(), BlockHashList: fileMetaData.GetBlockHashList()}
		latestVersion = fileMetaData.GetVersion()
	}

	return &Version{Version: latestVersion}, nil
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddr, error) {
	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
