SHELL:=bash

test:
	go test

test-3a:
	go test -run 3A

test-3a-untilfail:
	while make test-3a; do :; done

test-3b:
	go test -run 3B

test-3b-untilfail:
	while make test-3b; do :; done

time:
	time go test

time-3a:
	time go test -run 3A

time-3b:
	time go test -run 3B

race:
	go test -race

race-3a:
	go test -race -run 3A

race-3b:
	go test -race -run 3B


