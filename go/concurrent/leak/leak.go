package leak

import (
	"context"
	"log"
	"math/rand"
)

func process(v int) int {
	return v + rand.Intn(100)
}

func processContext(ctx context.Context, v int) int {
	if err := ctx.Err(); err != nil {
		return 0
	}

	return v + rand.Intn(100)
}

func processContextBatch(ctx context.Context, vals []int) []int {
	rands := make([]int, len(vals))

	// _ here is a digit separator, so its for readability
	for _, v := range vals {
		if err := ctx.Err(); err != nil {
			return rands
		}

		// Perform one unit of work.
		rands = append(rands, v+rand.Intn(100))
	}

	return rands
}

func processChannel(v int) <-chan int {
	ch := make(chan int)
	ch <- v + rand.Intn(100)
	return ch
}

func processContextSelect(ctx context.Context, v int) int {
	select {
	case <-ctx.Done():
		return 0
	case res := <-processChannel(v):
		return res
		// default:
		// 	return v + rand.Intn(100)
	}
}

// goroutine creation
// goroutine scheduling
// blocking goroutines
// runnable vs waiting goroutines
// goroutine stacks
// goroutine growth/shrinkage
// what causes goroutine leaks
// lifecycle ownership
// Classic leak
func worker(ch <-chan int) {
	// If nobody ever closes ch, this goroutine may live forever.
	for {
		v := <-ch
		process(v)
	}
}

// Better
func workerContext(ctx context.Context, ch <-chan int) {
	for {
		select {
		case <-ctx.Done():
			return
		case v, ok := <-ch:
			if !ok {
				return
			}
			
			process(v)
		}
	}
}

func blockSend() <-chan int {
	ch := make(chan int)

	func() {
		r := rand.Intn(100)
		ch <- r // Blocked forever, nothing receiving it
	}()

	return ch
}

func blockSendContext(ctx context.Context) <-chan int {
	ch := make(chan int)

	func() {
		r := process(rand.Int())

		select {
		case ch <- r:
		case <-ctx.Done():
			return
		}
	}()

	return ch
}

func blockReceive(ch <-chan int) {
	v := <-ch // Blocked forever, nothing is sending
	log.Printf("value: %d", v)
}

func Leaks() {
	res := blockSend() // this will block
	_ = res

	ch := make(chan int)
	func() {
		blockReceive(ch) // this will block, will try to read from channel which was never written value to
	}()
}
