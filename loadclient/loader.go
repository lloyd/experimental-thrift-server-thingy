// Autogenerated by Thrift Compiler (facebook)
// DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
// @generated

package main

import (
	"../gen-go/reverse"
	"flag"
	"fmt"
	thrift "github.com/facebook/fbthrift-go"
	"math"
	"os"
	"strconv"
	//	"sync"
	"time"
)

func Usage() {
	fmt.Fprintln(os.Stderr, "Usage of ", os.Args[0], " [-h host:port] [-u url] [-f[ramed]] function [arg1 [arg2...]]:")
	flag.PrintDefaults()
	fmt.Fprintln(os.Stderr, "\nFunctions:")
	fmt.Fprintln(os.Stderr, "  string Do(string input)")
	fmt.Fprintln(os.Stderr, "  void DoNothing()")
	fmt.Fprintln(os.Stderr, "  string DoReturn()")
	fmt.Fprintln(os.Stderr, "  void DoArg(string input)")
	fmt.Fprintln(os.Stderr)
	os.Exit(0)
}

func main() {
	flag.Usage = Usage
	var host string
	var protocol string
	var trans thrift.Transport
	_ = strconv.Atoi
	_ = math.Abs
	flag.Usage = Usage
	flag.StringVar(&host, "h", "10.0.7.190:8080", "Specify host and port")
	flag.StringVar(&protocol, "P", "binary", "Specify the protocol (binary, compact, simplejson, json)")
	flag.Parse()

	connectClient := func() (*reverse.ReverseClient, error) {
		var err error
		fmt.Printf("  >newsocket\n")
		trans, err = thrift.NewSocket(thrift.SocketAddr(host), thrift.SocketTimeout(time.Second*30))
		if err != nil {
			return nil, err
		}
		fmt.Printf("  >open\n")
		if err := trans.Open(); err != nil {
			return nil, err
		}
		fmt.Printf("  >protofact\n")
		var protocolFactory thrift.ProtocolFactory
		switch protocol {
		case "compact":
			protocolFactory = thrift.NewCompactProtocolFactory()
			break
		case "simplejson":
			protocolFactory = thrift.NewSimpleJSONProtocolFactory()
			break
		case "json":
			protocolFactory = thrift.NewJSONProtocolFactory()
			break
		case "binary", "":
			protocolFactory = thrift.NewBinaryProtocolFactoryDefault()
			break
		default:
			fmt.Fprintln(os.Stderr, "Invalid protocol specified: ", protocol)
			Usage()
			os.Exit(1)
		}
		fmt.Printf("  >clientfact\n")
		client := reverse.NewReverseClientFactory(trans, protocolFactory)
		fmt.Printf("  <clientfact\n")
		return client, nil
	}
	//	var wg sync.WaitGroup
	for i := 0; i < 10000; i++ {
		fmt.Printf(">connecting %d\n", i)
		client, err := connectClient()
		fmt.Printf("<connected\n")
		if err != nil {
			fmt.Fprintf(os.Stderr, "failure: %s\n", err)
			continue
		}

		//			for j := 0; j < 10; j++ {
		//		time.Sleep(time.Duration(i%20) + 30*time.Second)

		//fmt.Printf("%s\n", x)
		fmt.Printf(">doing\n")
		if _, err = client.Do(fmt.Sprintf("%d: this is a string", i)); err != nil {
			fmt.Printf("ERROR: %s\n", err)
		}
		//			}
		fmt.Printf(">closing\n")
		client.Close()
		fmt.Printf("<closed %d\n", i)
	}
	fmt.Printf("waiting for completion\n")
}