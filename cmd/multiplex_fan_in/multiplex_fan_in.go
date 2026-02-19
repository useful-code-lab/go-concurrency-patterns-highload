package main

import (
	"context"
	"fmt"
	"sync"
)

func Multiplex[T any](wg *sync.WaitGroup, ctx context.Context, sources ...<-chan T) <-chan T {
	resultCh := make(chan T)
	for _, source := range sources {
		wg.Add(1)

		go func(s <-chan T) {
			defer wg.Done()
			for {
				select {
				case data, ok := <-s:
					if !ok {
						return
					}
					select {
					case resultCh <- data:
					case <-ctx.Done():
						return
					}

				case <-ctx.Done():
					return
				}
			}
		}(source)
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	return resultCh
}

func main() {
	wg := sync.WaitGroup{}
	ctx := context.Background()

	source1 := make(chan int, 30)
	source2 := make(chan int, 30)
	source3 := make(chan int, 30)

	results := Multiplex(&wg, ctx, source1, source2, source3)

	go func() {
		for i := range 15 {
			source1 <- i
		}
		close(source1)
	}()

	go func() {
		for i := range 20 {
			source2 <- i * 2
		}
		close(source2)
	}()

	go func() {
		for i := range 30 {
			source3 <- i * 3
		}
		close(source3)
	}()

	count := 1
	for result := range results {
		fmt.Printf("%d-й элемент %d\n", count, result)
		count++
	}

	wg.Wait()
}
