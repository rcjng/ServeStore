# ServeStore

`ServeStore` is a file hosting service written in Go with distributed cloud storage that allows you to save and synchronize files of varying types and sizes from and to your computer(s).

The backend consists of two types of storage servers, one for file block data called BlockStore and the other for file metadata called MetaStore.

## Usage

1. Run the server using this:

```shell

go run cmd/server/main.go -s <service> -p <port> -l -d (BlockStoreAddr*)

```

Here, `service` should be one of three values: meta, block, or both. This is used to specify the service provided by the server. `port` defines the port number that the server listens to (default=8080). `-l` configures the server to only listen on localhost. `-d` configures the server to output log statements. Lastly, (BlockStoreAddr\*) is the BlockStore address that the server is configured with. If `service=both` then the BlockStoreAddr should be the `ip:port` of this server.

1. Run the client using this:

```shell

go run cmd/client/main.go -d <meta_addr:port> <base_dir> <block_size>
```

## Makefile

A makefile is provided to run the BlockStore and MetaStore servers.

1. Run both BlockStore and MetaStore servers (**listens to localhost on port 8081**):

```shell

make run

```

2. Run BlockStore server (**listens to localhost on port 8081**):

```shell

make run-blockstore

```

3. Run MetaStore server (**listens to localhost on port 8080**):

```shell

make run-metastore

```