package concurrent

import (
	"log"
	"sync"
)

// a simple concurrent program that emits numbers from a channel
func workerNumEmitter(ch <-chan int, wg *sync.WaitGroup) {
	defer wg.Done()

	for i := range ch {
		log.Printf("received: %d", i)
	}
}

func RunNumEmitter() {
	ch := make(chan int)
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go workerNumEmitter(ch, wg)

	go func() {
		for i := range 10 {
			ch <- i
		}
		close(ch)
	}()

	// wait for goroutine
	wg.Wait()
}
