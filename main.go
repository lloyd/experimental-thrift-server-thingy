package main

import (
	"fmt"
	"log"
	"os"
	"time"

	reverseif "./gen-go/reverse"
	thrift "github.com/facebook/fbthrift-go"
)

type ReverseImpl struct {
}

func (ri *ReverseImpl) Do(input string) (r string, err error) {
	//	log.Printf("Do called with %q", input)
	for i := 0; i < 1000000; i++ {

	}
	return input, nil
}

func (ri *ReverseImpl) DoArg(input string) (err error) {
	log.Printf("DoArg called with %q", input)
	return nil
}

func (ri *ReverseImpl) DoNothing() (err error) {
	log.Printf("DoNothing called")
	return nil
}

func (ri *ReverseImpl) DoReturn() (string, error) {
	log.Printf("DoReturn called")
	return "yo world", nil
}

func main() {
	processor := reverseif.NewReverseProcessor(&ReverseImpl{})
	server := NewThriftFramework().AddProcessor(processor).SetErrorLogger(
		func(format string, args ...interface{}) {
			log.Printf(format, args...)
		}).SetTooBusyException(func() thrift.WritableStruct {
		return &reverseif.TooBusyException{}
	})
	go func() {
		for {
			time.Sleep(time.Second)
			fmt.Printf("%v\n", server.Stats())
		}
	}()
	if err := server.Listen("10.0.0.7:8080"); err != nil {
		fmt.Fprintf(os.Stderr, "can't bind: %s", err)
		os.Exit(1)
	}
	if addr, err := server.Addr(); err != nil {
		log.Fatalf("addr reporting failure: %s\n", err)
	} else {
		log.Printf("bound to %s\n", addr.String())
	}

	if err := server.Serve(); err != nil {
		fmt.Fprintf(os.Stderr, "runtime error: %s\n", err)
		os.Exit(1)
	}

	/*
		processor := reverseif.NewReverseProcessor(&ReverseImpl{})
		transport, err := thrift.NewServerSocket("127.0.0.1:8080")
		if err != nil {
			fmt.Fprintf(os.Stderr, "can't bind: %s", err)
			os.Exit(1)
		}
		server := thrift.NewConcurrentServer(processor, transport)
		fmt.Printf("bound!\n")
		err = server.Listen()
		if err != nil {
			fmt.Fprintf(os.Stderr, "can't listen: %s", err)
			os.Exit(1)
		}
		server.Serve()
	*/

}
