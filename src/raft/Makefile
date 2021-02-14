test:
	go test

test-2a:
	go test -run 2A

test-2a-untilfail:
	while make test-2a; do :; done

test-2b:
	go test -run 2B

test-2b-untilfail:
	while make test-2b; do :; done

test-2c:
	go test -run 2C

time-2b:
	time go test -run 2B

race:
	go test -race

race-2a:
	go test -race -run 2A

race-2a-untilfail:
	while make race-2a; do :; done

race-2b:
	go test -race -run 2B

race-2c:
	go test -race -run 2C

