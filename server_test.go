package lwes

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

func TestBufferRetention(t *testing.T) {
	msg := `test`
	pool := &sync.Pool{}
	reader := strings.NewReader(msg)
	buffer1 := NewFixedBuffer(pool, len(msg))
	_, err := buffer1.ReadFrom(reader)
	if err != nil {
		t.Fatalf("Unexpected error while writing to buffer %v", err)
	}

	if string(buffer1.Bytes()) != msg {
		t.Fatalf(`Unexpected message written "%s"`, string(buffer1.Bytes()))
	}
	buffer1.Done()
	buffer2 := NewFixedBuffer(pool, len(msg))
	if string(buffer2.Bytes()) != "" {
		t.Fatalf(`Unexpected data buffered "%s"`, string(buffer2.Bytes()))
	}
}

func TestBufferMultWrite(t *testing.T) {
	msg := `test`
	reader := strings.NewReader(msg)
	buffer := NewFixedBuffer(&sync.Pool{}, 100)
	buffer.ReadFrom(reader)
	reader.Seek(0, 0)
	buffer.ReadFrom(reader)
	expected := fmt.Sprintf("%s%s", msg, msg)
	if string(buffer.Bytes()) != expected {
		t.Fatalf("Unexpected written message: %s != %s", string(buffer.Bytes()), expected)
	}
}
