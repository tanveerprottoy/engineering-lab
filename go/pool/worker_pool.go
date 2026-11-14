package pool

import (
	"context"
	"sync"
)

type Job[T any] func(ctx context.Context) (T, error)

type Result[T any] struct {
	Index int
	Val   T
	Err   error
}

// Run executes jobs with at most `workers` running at once.
// Results are returned in input order.
func Run[T any](ctx context.Context, workers int, jobs []Job[T]) []Result[T] {
	results := make([]Result[T], len(jobs)) // each index written by exactly one goroutine
	work := make(chan int)                  // carries job indices
	var wg sync.WaitGroup

	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for i := range work {
				if err := ctx.Err(); err != nil {
					results[i] = Result[T]{Index: i, Err: err}
					continue
				}
				v, err := jobs[i](ctx)
				results[i] = Result[T]{Index: i, Val: v, Err: err}
			}
		}()
	}

feed:
	for i := range jobs {
		select {
		case work <- i:
		case <-ctx.Done():
			// Mark everything not yet dispatched as cancelled.
			for j := i; j < len(jobs); j++ {
				results[j] = Result[T]{Index: j, Err: ctx.Err()}
			}
			break feed
		}
	}
	close(work) // lets workers' range loops end
	wg.Wait()
	return results
}

func JobRunner() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	jobs := make([]Job[string], 0, len(urls))
	for _, u := range urls {
		jobs = append(jobs, func(ctx context.Context) (string, error) {
			return fetch(ctx, u)
		})
	}
	res := pool.Run(ctx, 8, jobs)
}
