package main

import (
	"fmt"
	"sync"
)

type Task struct {
	ID int
}

func WorkerPool(jobs chan Task, wg *sync.WaitGroup) chan Task {
	result := make(chan Task, 100)
	for range 5 {
		wg.Add(1)
		go func(j chan Task) {
			defer wg.Done()
			for task := range j {
				fmt.Printf("Обрабатывается задача %d\n", task.ID)
				result <- task
			}

		}(jobs)
	}

	go func() {
		wg.Wait()
		close(result)
	}()

	return result
}

func main() {
	var wg sync.WaitGroup
	jobs := make(chan Task, 10)

	result := WorkerPool(jobs, &wg)

	go func() {
		for i := range 40 {
			jobs <- Task{ID: i}
		}
		close(jobs)
	}()

	for task := range result {
		fmt.Printf("Обработана задача %d\n", task.ID)
	}
}
