package concurrent

import "log"

// a simple concurrent program that behaves like round robin
func workerRound(ch <-chan int) {
	log.Printf("round: %d", <-ch)
}

func workerRobin(ch <-chan int) {
	log.Printf("robin: %d", <-ch)
}

func RunRoundRobin() {
	ro := make(chan int)
	rb := make(chan int)

	go workerRound(ro)
	go workerRobin(rb)

	go func() {
		for i := range 10 {
			if i%2 == 0 {
				ro <- i
			} else {
				rb <- i
			}
		}
		close(ro)
		close(rb)
	}()
}
