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
			for task := range j {
				fmt.Printf("Обрабатывается задача %d\n", task.ID)
				result <- task
			}
			wg.Done()
		}(jobs)
	}

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

	wg.Wait()
	close(result)

	fmt.Printf("\n\nОбработанные задачи:\n")
	for task := range result {
		fmt.Printf("Обработана задача %d\n", task.ID)
	}
}
