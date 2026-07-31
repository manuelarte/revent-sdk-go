tools:
	go install github.com/bufbuild/buf/cmd/buf@v1.72.0
	go install gotest.tools/gotestsum@latest

lint: fmt
	golangci-lint custom -vv
	./custom-gcl run --fix ./...

fmt:
	golangci-lint fmt

test:
	gotestsum ./...
