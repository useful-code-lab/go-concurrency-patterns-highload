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

	sources := make(chan int)
	wg.Add(len(sources))

	

	wg.Wait()

}
