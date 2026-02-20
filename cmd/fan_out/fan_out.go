package main

import (
	"fmt"
	"sync"
)

type Task struct {
	ID int
}

func WorkerPool(jobs chan Task) {
	for range 5 {
		
		go func(j chan Task) {
			for {
				select {
				case task := <-j:
					fmt.Printf("Обрабатывается задача %d", task.ID)
				}
			}
		}(jobs)
	}
}

func main() {
	wg := sync.WaitGroup{}
	jobs := make(chan Task, 10)

	for i := range 12 {
		jobs <- Task{ID: i * 12}
	}

	func() {
		wg.Wait()
	}()

}
