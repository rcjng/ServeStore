.PHONY: install
install:
	rm -rf bin
	GOBIN=$(PWD)/bin go install ./...

.PHONY: run
run:
	go run cmd/server/main.go -s both -p 8081 -l localhost:8081

.PHONY: run-blockstore
run-blockstore:
	go run cmd/server/main.go -s block -p 8081 -l

.PHONY: run-metastore
run-metastore:
	go run cmd/server/main.go -s meta -l localhost:8081
