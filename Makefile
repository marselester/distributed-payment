build:
	go build ./cmd/accountantd/
	go build ./cmd/paymentd/
	go build ./cmd/transfer-server/

fmt:
	go fmt ./...

lint:
	golint ./rest ./mock ./cmd/transfer-server ./cmd/paymentd ./cmd/accountantd

test:
	go test ./...
