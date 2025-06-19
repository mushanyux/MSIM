package msnet

type Buffer interface {
	// IsEmpty returns true if the buffer is empty.
	IsEmpty() bool
	// Write writes the data to the buffer.
	Write(data []byte) (int, error)
	// Read reads the data from the buffer.
	Read(data []byte) (int, error)
	// BoundBufferSize returns the bound buffer size.
	BoundBufferSize() int
	// Peek returns the data from the buffer without removing it.
	Peek(n int) (head []byte, tail []byte)
	PeekBytes(p []byte) int
	// Discard discards the data from the buffer.
	Discard(n int) (int, error)
	// Release releases the buffer.
	Release() error
}

type InboundBuffer interface {
	Buffer
}

type OutboundBuffer interface {
	Buffer
}

type DefaultBuffer struct {
	ringBuffer *RingBuffer
}

func NewDefaultBuffer() *DefaultBuffer {
	return &DefaultBuffer{
		ringBuffer: &RingBuffer{},
	}
}

func (d *DefaultBuffer) IsEmpty() bool {
	return d.ringBuffer.IsEmpty()
}

func (d *DefaultBuffer) Write(data []byte) (int, error) {
	return d.ringBuffer.Write(data)
}
func (d *DefaultBuffer) Read(data []byte) (int, error) {
	return d.ringBuffer.Read(data)
}
func (d *DefaultBuffer) BoundBufferSize() int {
	return d.ringBuffer.Buffered()
}

func (d *DefaultBuffer) Peek(n int) (head []byte, tail []byte) {
	return d.ringBuffer.Peek(n)
}

func (d *DefaultBuffer) PeekBytes(p []byte) int {
	head, tail := d.ringBuffer.Peek(-1)
	if len(head) > 0 && len(tail) > 0 {
		return copy(p, append(head, tail...))
	}

	if len(head) > 0 {
		return copy(p, head)
	}
	if len(tail) > 0 {
		return copy(p, tail)
	}
	return 0
}

func (d *DefaultBuffer) Discard(n int) (int, error) {
	return d.ringBuffer.Discard(n)
}

func (d *DefaultBuffer) Release() error {
	d.ringBuffer.Done()
	d.ringBuffer.Reset()
	return nil
}
