package main

import (
	"fmt"
	"sync"
)

type Task struct {
	ID int
}

func WorkerPool(jobs chan Task, wg *sync.WaitGroup) {
	for range 5 {
		wg.Add(1)
		go func(j chan Task) {
			for task := range j {
				fmt.Printf("Обрабатывается задача %d\n", task.ID)
			}
			wg.Done()
		}(jobs)
	}
}

func main() {
	var wg sync.WaitGroup
	jobs := make(chan Task, 10)

	WorkerPool(jobs, &wg)

	for i := range 40 {
		jobs <- Task{ID: i}
	}

	close(jobs)

	wg.Wait()
}
