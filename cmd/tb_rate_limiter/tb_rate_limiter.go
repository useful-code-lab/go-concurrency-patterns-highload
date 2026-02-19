package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var rateN int
var periodT int
var taskQueue chan Task
var ticker *time.Ticker

type Task struct {
	ID int
}

func (t *Task) Process() {
	fmt.Printf("Задача %d запущена\n", t.ID)
}

func Init() {
	rateN = 50  // Количество доступных "токенов" (разрешений на выполнение) за период.
	periodT = 1 // Временной интервал (например, 1 секунда).
	taskQueue = make(chan Task)
	ticker = time.NewTicker(2 * time.Second)
}

func AddTask(task Task) {
	taskQueue <- task
}

func Run() {
	for {
		select {
		case <-ticker.C:
			select {
			case task, ok := <-taskQueue:
				if ok {
					go task.Process()
				}
			default:
			}
		case <-context.Background().Done():
			ticker.Stop()
			return
		}
	}
}

func main() {
	context, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	Init()
	go Run()

	for i := range 10 {
		AddTask(Task{ID: i + 1})
	}

	context.Done()
}
