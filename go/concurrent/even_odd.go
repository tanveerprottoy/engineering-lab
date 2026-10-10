package concurrent

import (
	"log"
	"sync"
)

// a simple concurrent program that behaves like round robin
func workerEven(ch <-chan int, wg *sync.WaitGroup) {
	defer wg.Done()

	log.Printf("even: %d", <-ch)
}

func workerOdd(ch <-chan int, wg *sync.WaitGroup) {
	defer wg.Done()

	log.Printf("odd: %d", <-ch)
}

func RunEvenOdd() {
	wg := new(sync.WaitGroup)

	evenCh := make(chan int)
	oddCh := make(chan int)

	wg.Add(1)
	go workerEven(evenCh, wg)

	wg.Add(1)
	go workerOdd(oddCh, wg)

	go func() {
		for i := range 10 {
			if i%2 == 0 {
				evenCh <- i

			} else {
				oddCh <- i
			}
		}
		close(evenCh)
		close(oddCh)
	}()
}
