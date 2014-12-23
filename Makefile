.PHONY: test livetest

test:
	go test

livetest:
	#go test -tags live
	go test -tags live -run TestLiveIndex
