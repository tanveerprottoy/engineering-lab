package concurrent

import (
	"log"
	"sync"
)

// a simple concurrent program, that will multiply with last value in a goroutine and summed up to
// a total in another, communicated through channel
var (
	mu sync.Mutex

	multi = 0
	sum   = 0
)

func multiplier(ch <-chan int) {
	mu.Lock()
	defer mu.Unlock()
	v := <-ch
	multi *= v
}

func adder(ch <-chan int) {
	mu.Lock()
	defer mu.Unlock()
	v := <-ch
	sum += v
}

func RunConcurrentArithmetics() {
	ch := make(chan int)

	adder(ch)
	multiplier(ch)

	for i := range 10 {
		ch <- i
	}

	log.Printf("multi: %d\nsum: %d", multi, sum)
}
