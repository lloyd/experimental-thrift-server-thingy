all: tserver reverse-remote

.PHONY: thrift1
thrift1:
	make -C /Users/lth/dev/fbthrift/build -j4 thrift

.PHONY: gen_code
gen_code: thrift1
	/Users/lth/dev/fbthrift/build/bin/thrift1 -gen go reverse.thrift

tserver: gen_code
	go build

reverse-remote: gen_code
	go build ./gen-go/reverse/reverse-remote/

