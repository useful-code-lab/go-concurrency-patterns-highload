package main

import (
	"context"
	"sync"
)

func Multiplex[T any](wg *sync.WaitGroup, sources ...<-chan T) {
	resultCh := make(chan T)

}

func main() {
	wg := sync.WaitGroup{}
	ctx := context.Background()

	wg.Add(3)

	source1 := make(chan int)
	source2 := make(chan int, 30)
	source3 := make(chan int)

	for i := range 15 {
		source1 <- i
	}

	for i := range 20 {
		source1 <- i * 2
	}

	for i := range 30 {
		source3 <- i * 3
	}

	Multiplex(&wg, source1, source2, source3)

	wg.Wait()

}
