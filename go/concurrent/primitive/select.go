package primitive

import (
	"context"
	"fmt"
	"log"
	"time"

	"golang.org/x/sync/errgroup"
)

// Job represents a small unit of work used by the channel examples.
type Job struct {
	ID int
}

func (j Job) Run(ctx context.Context) error {
	return nil
}

// blockingReceiveExample shows select without default.
// It blocks the current goroutine if no case is ready.
// A sender delivers a value after a short delay, so this example completes.
func blockingReceiveExample() {
	ch := make(chan int)

	go func() {
		time.Sleep(100 * time.Millisecond)
		ch <- 42
	}()

	select {
	case value := <-ch:
		fmt.Println("received:", value)
	}
}

// nonBlockingReceiveExample attempts to receive immediately.
// Because default is present, select does not wait if the channel is not ready.
func nonBlockingReceiveExample() {
	ch := make(chan int)

	select {
	case value := <-ch:
		fmt.Println("received:", value)
	default:
		fmt.Println("no value ready; did not block")
	}

	// A buffered value is ready, so the receive case wins instead of default.
	buffered := make(chan int, 1)
	buffered <- 7

	select {
	case value := <-buffered:
		fmt.Println("received buffered value:", value)
	default:
		fmt.Println("this line will not run for this select")
	}
}

// nonBlockingSendExample tries to send without waiting.
// An unbuffered channel needs a receiver ready at the same time.
// With no receiver, default runs immediately.
func nonBlockingSendExample() {
	ch := make(chan int)

	select {
	case ch <- 99:
		fmt.Println("send succeeded")
	default:
		fmt.Println("no receiver ready; send skipped without blocking")
	}

	// A buffered channel with free capacity can accept a send immediately.
	buffered := make(chan int, 1)
	select {
	case buffered <- 100:
		fmt.Println("buffered send succeeded")
	default:
		fmt.Println("buffer full; send skipped")
	}
}

// semaphoreExample uses a buffered channel as a capacity limiter.
// A successful send acquires a slot; receiving releases it.
// tryAcquire never waits: it returns false when all slots are occupied.
func semaphoreExample() {
	semaphore := make(chan struct{}, 2)

	tryAcquire := func() bool {
		select {
		case semaphore <- struct{}{}:
			return true
		default:
			return false
		}
	}

	release := func() {
		<-semaphore
	}

	fmt.Println("acquire 1:", tryAcquire()) // true
	fmt.Println("acquire 2:", tryAcquire()) // true
	fmt.Println("acquire 3:", tryAcquire()) // false: capacity is full

	release()
	fmt.Println("acquire after release:", tryAcquire()) // true
}

// contextCheckExample checks cancellation without waiting.
// For a simple check, ctx.Err() is usually clearer than select + default.
func contextCheckExample() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fmt.Println("before cancel, canceled?", ctx.Err() != nil) // false
	cancel()
	fmt.Println("after cancel, canceled?", ctx.Err() != nil) // true

	// The equivalent non-blocking select pattern:
	select {
	case <-ctx.Done():
		fmt.Println("select check: context is canceled")
	default:
		fmt.Println("select check: context is not canceled")
	}
}

// resultOrCancellationExample waits for either a result or cancellation.
// The worker also observes the context while trying to send, preventing it
// from being stranded forever on an unbuffered send if the caller has left.
func resultOrCancellationExample() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resultCh := make(chan int)

	go func() {
		// Simulate work that takes some time.
		select {
		case <-time.After(50 * time.Millisecond):
			result := 123
			// Sending is cancellation-aware. If the caller has canceled,
			// the worker can exit rather than block forever on this send.
			select {
			case resultCh <- result:
			case <-ctx.Done():
			}
		case <-ctx.Done():
			return
		}
	}()

	select {
	case <-ctx.Done():
		fmt.Println("context canceled before result")
	case result := <-resultCh:
		fmt.Println("received result:", result)
	}
}

// timeoutExample waits for a result up to a fixed duration.
// Unlike default, the timer case allows select to block for up to the timeout.
func timeoutExample() {
	resultCh := make(chan string)

	go func() {
		time.Sleep(50 * time.Millisecond)
		resultCh <- "ready"
	}()

	select {
	case result := <-resultCh:
		fmt.Println("result:", result)
	case <-time.After(200 * time.Millisecond):
		fmt.Println("timed out")
	}
}

// priorityExample first checks urgent work non-blockingly.
// If urgent work is already ready, it handles it before normal work.
// Otherwise it blocks waiting for either channel. This is best-effort priority,
// not strict priority: if both become ready during the second select, Go may
// choose either case. This small demo runs a single selection rather than a
// persistent worker loop, so channel shutdown handling is not needed here.
func priorityExample() {
	urgent := make(chan Job, 1)
	normal := make(chan Job, 1)

	// Make both jobs ready before the worker starts, so the first select
	// demonstrates that already-ready urgent work is preferred.
	urgent <- Job{ID: 1}
	normal <- Job{ID: 2}

	handle := func(label string, job Job) {
		fmt.Printf("%s job: %d\n", label, job.ID)
	}

	// First, opportunistically check urgent work.
	select {
	case job := <-urgent:
		handle("urgent", job)
	default:
		// No urgent job was ready at this instant.
		// Fall through to the blocking selection below.
		select {
		case job, ok := <-urgent:
			if ok {
				handle("urgent", job)
			}
		case job, ok := <-normal:
			if ok {
				handle("normal", job)
			}
		}
	}

	// Consume any remaining normal job to keep the example tidy.
	select {
	case job := <-normal:
		handle("normal", job)
	default:
	}
}

// loopedSelect keeps running until ctx is done o an error is returned
func loopedSelect(ctx context.Context, jobs <-chan Job) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case j, ok := <-jobs:
			if !ok {
				return nil
			}
			if err := j.Run(ctx); err != nil {
				return err
			}
		}
	}
}

// RunSelectExamples calls each example in a predictable order.
// Run this program with: go run select_examples.go
func RunSelectExamples() {
	fmt.Println("\n=== 1. Blocking select: wait for a value ===")
	blockingReceiveExample()

	fmt.Println("\n=== 2. Non-blocking receive ===")
	nonBlockingReceiveExample()

	fmt.Println("\n=== 3. Non-blocking send ===")
	nonBlockingSendExample()

	fmt.Println("\n=== 4. Channel-based semaphore: try to acquire ===")
	semaphoreExample()

	fmt.Println("\n=== 5. Non-blocking context cancellation check ===")
	contextCheckExample()

	fmt.Println("\n=== 6. Wait for a result or cancellation ===")
	resultOrCancellationExample()

	fmt.Println("\n=== 7. Wait for a result or timeout ===")
	timeoutExample()

	fmt.Println("\n=== 8. Prefer urgent work when it is already ready ===")
	priorityExample()
}

func RunLoopedSelect() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// For fan-out with error propagation, use golang.org/x/sync/errgroup. The first error cancels the derived context, and Wait() returns that error.
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(8) // bounded concurrency
	for _, u := range urls {
		g.Go(func() error { return fetch(ctx, u) })
	}
	if err := g.Wait(); err != nil {
		log.Printf("error: %s", err.Error())
	}
}
