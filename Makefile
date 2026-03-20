.PHONY:gen-code

gen-code:
	protoc -I=. --go_out=. --go_opt=paths=source_relative  ./pb/commonpb/common.proto

build:

ut:
	go test -coverprofile cover.out ./...
	go tool cover -func=cover.out | tail -1  | grep -P '\d+\.\d+(?=\%)' -o

lint:
	golangci-lint run ./...

comment:
	gocloc --include-lang=Go --output-type=json --not-match=".*_test.go|types.go" --not-match-d="pb|test" . | jq '(.total.comment-.total.files*5)/(.total.code+.total.comment)*100'

pre-commit: lint ut comment

update-mod:
	go get github.com/ethereum/go-ethereum@v1.14.11
	go get github.com/jackz-jones/notification-contract-go@dev
	go get github.com/jackz-jones/nft-contract-go@dev
	go get chainmaker.org/chainmaker/sdk-go/v2@v2.3.8
	go get chainmaker.org/chainmaker/contract-sdk-go/v2@v2.3.9
	go mod tidy