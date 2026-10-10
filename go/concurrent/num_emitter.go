package concurrent

import (
	"log"
	"sync"
)

// a simple concurrent program that emits numbers from a channel
func workerNumEmitter(ch <-chan int, wg *sync.WaitGroup, prefix string) {
	defer wg.Done()

	for i := range ch {
		log.Printf("on %s received: %d", prefix, i)
	}
}

func RunNumEmitter() {
	ch := make(chan int)
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go workerNumEmitter(ch, wg, "worker 0")

	wg.Add(1)
	go workerNumEmitter(ch, wg, "worker 1")

	wg.Add(1)
	go workerNumEmitter(ch, wg, "worker 2")

	go func() {
		for i := range 10 {
			ch <- i
		}
		close(ch)
	}()

	// wait for goroutines
	wg.Wait()
}
