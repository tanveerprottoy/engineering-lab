package concurrent

import (
	"crypto/rand"
	"log"
	"sync"
)

type job struct {
	name  string
	value int
}

// a simple concurrent program that emits numbers from a channel
func worker(ch <-chan job, wg *sync.WaitGroup, prefix string) {
	defer wg.Done()

	for j := range ch {
		log.Printf("on %s job: %v", prefix, j)
	}
}

func RunWorkerPool() {
	ch := make(chan job)
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go worker(ch, wg, "worker 0")

	wg.Add(1)
	go worker(ch, wg, "worker 1")

	wg.Add(1)
	go worker(ch, wg, "worker 2")

	go func() {
		for i := range 10 {
			ch <- job{
				name:  rand.Text(),
				value: i,
			}
		}
		close(ch)
	}()

	// wait for goroutines
	wg.Wait()
}
