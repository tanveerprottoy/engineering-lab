package concurrent

import (
	"log"
	"runtime"
)

func GoRoutine() {
	log.Printf("goroutines: %d", runtime.NumGoroutine())
	log.Printf("cpus: %d", runtime.NumCPU())
}
