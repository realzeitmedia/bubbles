.PHONY: test livetest vet lint

all: test vet lint

test:
	go test

livetest:
	#go test -tags live
	go test -tags live -run TestLiveIndex

vet:
	go vet

lint:
	golint .
