package squash

import "testing"
import "fmt"
import "time"

func TestPipe(t *testing.T) {
	p := newPipe()

	go func() {
		b := make([]byte, 1)

		for {
			time.Sleep(time.Second)
			p.Read(b)
			fmt.Println(string(b))
		}
	}()

	for {
		p.Write([]byte("a"))

	}
}
