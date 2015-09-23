package squash

import "bytes"
import "sync"

type pipe struct {
	buf      *bytes.Buffer
	m        sync.Mutex
	cond     *sync.Cond
	closed   bool
	errClose error
}

func newPipe() *pipe {
	p := &pipe{
		buf: bytes.NewBuffer(make([]byte, 0, 256)),
	}
	p.cond = sync.NewCond(&p.m)
	return p
}

func (p *pipe) Read(b []byte) (int, error) {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()

	for p.buf.Len() < len(b) && !p.closed {
		p.cond.Wait()
	}

	if p.errClose != nil {
		return 0, p.errClose
	}

	return p.buf.Read(b)
}

func (p *pipe) Write(b []byte) (int, error) {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()
	defer p.cond.Signal()

	return p.buf.Write(b)
}

func (p *pipe) Close(err error) {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()
	defer p.cond.Signal()

	p.errClose = err
	p.closed = true
}
