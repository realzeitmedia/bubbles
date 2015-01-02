.PHONY: test livetest vet lint

all: test vet lint

test:
	go test

livetest:
	go test -tags live -v

vet:
	go vet *.go

lint:
	golint .
