package main

import (
	"fmt"
	"net"
	//	"runtime"
	"sync"
	"sync/atomic"

	thrift "github.com/facebook/fbthrift-go"
)

type ThriftLogFunction func(format string, args ...interface{})

type ThriftFramework struct {
	pmap         map[string]thrift.ProcessorFunction
	ln           net.Listener
	connCount    int64
	workingCount int64
	writerCount  int64
	workerCount  int
	backlogSize  int
	workchan     chan work
	wg           sync.WaitGroup
	logfunc      ThriftLogFunction
}

type Processor interface {
	ProcessorMap() map[string]thrift.ProcessorFunction
}

func NewThriftFramework() *ThriftFramework {
	return &ThriftFramework{
		pmap:        map[string]thrift.ProcessorFunction{},
		workerCount: 1, //runtime.NumCPU() * 2, // XXX: configurable concurrency!?
		backlogSize: 0, // runtime.NumCPU() * 2,
	}
}

type TFStats struct {
	// the number of reader goroutines (i.e. connections allocated)
	ReaderCount int64
	// the number of writer goroutines (i.e. connections allocated)
	WriterCount int64
	// the number of processing go-routines (i.e. doing actual work)
	WorkingCount int64
}

func (tf *ThriftFramework) Stats() (stats TFStats) {
	stats.ReaderCount = atomic.LoadInt64(&tf.connCount)
	stats.WriterCount = atomic.LoadInt64(&tf.writerCount)
	stats.WorkingCount = atomic.LoadInt64(&tf.workingCount)
	return
}

func (tf *ThriftFramework) SetErrorLogger(logfunc ThriftLogFunction) *ThriftFramework {
	tf.logfunc = logfunc
	return tf
}

func (tf *ThriftFramework) AddProcessor(p Processor) *ThriftFramework {
	for name, fn := range p.ProcessorMap() {
		tf.pmap[name] = fn
	}
	return tf
}

func (tf *ThriftFramework) Listen(addr string) error {
	if tf.ln != nil {
		return fmt.Errorf("already bound to %s", tf.ln.Addr().String())
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	tf.ln = ln
	return nil
}

func (tf *ThriftFramework) Addr() (net.Addr, error) {
	if tf.ln == nil {
		return nil, fmt.Errorf("not bound")
	}
	return tf.ln.Addr(), nil
}

func (tf *ThriftFramework) worker(wchan <-chan work) {
	for wrk := range wchan {
		atomic.AddInt64(&tf.workingCount, 1)
		//		fmt.Printf("  worker <- %s\n", wrk.request)
		response, err := wrk.pf.Run(wrk.request)
		if err != nil {
			tf.reportError("error handling %s message: %s", wrk.name, err)
		}
		//		fmt.Printf("  worker -> %s\n", response)
		wrk.response = response
		wrk.writechan <- wrk
		atomic.AddInt64(&tf.workingCount, -1)
	}
}

func (tf *ThriftFramework) Serve() error {
	if tf.ln == nil {
		return fmt.Errorf("Serve() called without Listen()")
	}

	tf.workchan = make(chan work, tf.backlogSize)
	for i := 0; i < tf.workerCount; i++ {
		tf.wg.Add(1)
		go tf.worker(tf.workchan)
	}

	// accept loop
	for {
		conn, err := tf.ln.Accept()
		if err != nil {
			tf.reportError("during accept: %s", err)
		} else {
			// XXX: check max connnections here and early loadshed
			go tf.reader(conn)
		}
	}
	close(tf.workchan)
	tf.wg.Wait()

	return nil
}

type work struct {
	pf        thrift.ProcessorFunction
	name      string
	seqId     int32
	request   thrift.Struct
	response  thrift.WritableStruct
	writechan chan<- work
}

func (tf *ThriftFramework) writer(prot thrift.Protocol, wchan <-chan work) {
	atomic.AddInt64(&tf.writerCount, 1)
	defer atomic.AddInt64(&tf.writerCount, -1)

	for wrk := range wchan {
		wrk.pf.Write(wrk.seqId, wrk.response, prot)
	}
}

func (tf *ThriftFramework) reader(conn net.Conn) {
	atomic.AddInt64(&tf.connCount, 1)
	defer atomic.AddInt64(&tf.connCount, -1)

	// XXX: SSL upgrade

	transport, err := thrift.NewSocket(thrift.SocketConn(conn))
	if transport != nil {
		defer transport.Close()
	}
	if err != nil {
		tf.reportError("while allocating transport: %s", err)
		return
	}
	prot := thrift.NewBinaryProtocolTransport(transport)

	// now that we've accepted the connection, let's spin up a
	// write goroutine. NOTE: golang best practice for a hot server
	// is this.  one go-routine for reading, one for writing.
	ochan := make(chan work)
	defer close(ochan)
	go tf.writer(prot, ochan)

	for {
		name, _, seqId, err := prot.ReadMessageBegin()
		if err != nil {
			if err, ok := err.(thrift.TransportException); ok && err.TypeID() == thrift.END_OF_FILE {
				// connectionn terminated because client closed connection
				break
			}
			tf.reportError("error reading message begin: %s", err)
			break
		}
		pfunc, ok := tf.pmap[name]
		if !ok {
			prot.Skip(thrift.STRUCT)
			prot.ReadMessageEnd()
			exc := thrift.NewApplicationException(thrift.UNKNOWN_METHOD, "Unknown function "+name)
			// XXX: concurrency error.  We really need to be writing this on the connection's
			// write goroutine
			fmt.Printf("RISKY BEHAVIOR\n")
			prot.WriteMessageBegin(name, thrift.EXCEPTION, seqId)
			exc.Write(prot)
			prot.WriteMessageEnd()
			prot.Flush()
			continue
		}
		request, err := pfunc.Read(seqId, prot)
		if err != nil {
			tf.reportError("while reading %s message: %s", err)
			// terminate connection when we fail to read a protocol message.  it is a protocol
			// violation we cannot recover from
			break
		}

		wrk := work{
			pf:        pfunc,
			seqId:     seqId,
			name:      name,
			request:   request,
			writechan: ochan,
		}

		// now lets' try to process said work
		// XXX: buffered chan with non-blocking write
		tf.workchan <- wrk
	}
}

func (tf *ThriftFramework) reportError(format string, a ...interface{}) {
	if tf.logfunc != nil {
		tf.logfunc(format, a...)
	}
}
