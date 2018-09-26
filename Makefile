run:
	go run cmd/modb/modb.go --store=mo.db

help:
	go run cmd/modb/modb.go --help

build:
	go build cmd/modb/modb.go

client:
	redis-cli -p 6380

clean:
	rm modb
