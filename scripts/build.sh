cd ../
go mod verify
go mod tidy
go build -o ./bin/bench ./cmd/bench/main.go
echo "ok"