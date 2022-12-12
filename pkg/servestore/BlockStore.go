package servestore

import (
	context "context"
	"errors"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	if blockHash == nil {
		return nil, errors.New("ErrNilBlockHash")
	}

	if block, exists := bs.BlockMap[blockHash.GetHash()]; exists {
		return block, nil
	} else {
		return nil, errors.New("ErrBlockNotFound")
	}
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	success := &Success{}
	if block == nil {
		success.Flag = false
		return success, errors.New("ErrNilBlock")
	}

	bs.BlockMap[GetBlockHashString(block.GetBlockData())] = &Block{BlockData: block.GetBlockData(), BlockSize: block.GetBlockSize()}
	success.Flag = true
	return success, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	if blockHashesIn == nil {
		return nil, errors.New("ErrNilBlockHashes")
	}

	blockHashes := &BlockHashes{Hashes: make([]string, 0)}
	for _, hash := range blockHashesIn.GetHashes() {
		if _, exists := bs.BlockMap[hash]; exists {
			blockHashes.Hashes = append(blockHashes.GetHashes(), hash)
		}
	}

	return blockHashes, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
