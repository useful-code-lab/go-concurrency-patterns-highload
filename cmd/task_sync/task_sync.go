package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Task interface {
	Execute(ctx context.Context) error
	GetID() int
}

type EmailTask struct {
	ID     int
	Result string
}

func (e *EmailTask) GetID() int {
	return e.ID
}

func (e *EmailTask) Execute(ctx context.Context) error {
	select {
	case <-time.After(time.Second * 2):
		fmt.Printf("Почта отправлена %d", e.ID)
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

type DataTask struct {
	ID     int
	Result string
}

func (e *DataTask) GetID() int {
	return e.ID
}

func (d *DataTask) Execute(ctx context.Context) error {
	steps := []string{"cleaning", "tranforming", "saving"}

	for _, step := range steps {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			fmt.Printf("Шаг %d запущен для таска %d", step, d.ID)
			time.Sleep(5 * time.Millisecond)
		}
	}

	return nil
}

type Dispatcher struct {
	JobQueue   chan Task
	MaxWorkers int
	wg         sync.WaitGroup
	Results    chan Task
}

func (d *Dispatcher) Worker(ctx context.Context) {
	for {
		select {
		case task := <-d.JobQueue:
			task.Execute(ctx)
			d.wg.Done()
		case <-ctx.Done():
			return
		}
	}
}

func (d *Dispatcher) Start(ctx context.Context) {
	for range d.MaxWorkers {
		d.wg.Add(1)
		go d.Worker(ctx)
	}
}

func (d *Dispatcher) Wait() {
	close(d.JobQueue)
	close(d.Results)
	d.wg.Wait()
}

func newDispatcher() *Dispatcher {
	return &Dispatcher{
		JobQueue:   make(chan Task),
		MaxWorkers: 5,
		Results:    make(chan Task),
	}
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	dispatcher := newDispatcher()
	dispatcher.Start(ctx)

	go func() {
		for result := range dispatcher.Results {
			fmt.Printf("Результат выполнения задачи %d", result.GetID())
		}
	}()

Loop:
	for i := range 20 {
		var task Task
		if i%2 == 0 {
			task = &EmailTask{ID: i}
		} else {
			task = &DataTask{ID: i}
		}

		select {
		case dispatcher.JobQueue <- task:
		case <-ctx.Done():
			break Loop
		}

		if i == 15 {
			fmt.Printf("Отменяем оставшиеся задачи %d", i)
			cancel()
		}
	}

	dispatcher.Wait()
}
