build-wc: ../mrapps/wc.go
	go build -buildmode=plugin ../mrapps/wc.go

run-seq: build-wc
	rm -f mr-out* ;\
	go run mrsequential.go wc.so pg*.txt ;\
	cat mr-out-0 | sort | more

run-master:
	rm -f mr-out* ;\
	go run mrmaster.go pg-*.txt; \
	cat mr-out-* | sort | more

run-worker: build-wc
	go run mrworker.go wc.so

test:
	sh test-mr.sh