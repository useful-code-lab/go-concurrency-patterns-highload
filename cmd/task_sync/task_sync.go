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
}

type EmailTask struct {
	ID     int
	Result string
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

func (d *DataTask) Execute(ctx context.Context) error {
	steps := []string{"cleaning", "tranforming", "saving"}

	for _, step := range steps {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			fmt.Printf("Шаг %d запущен для таска %d", step, d.ID)
			time.Sleep(5 * time.Second)
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
		case <-ctx.Done():
			return
		}
	}
}

func (d *Dispatcher) Start(ctx context.Context) {
	for {
		select {
		case task := <-d.JobQueue:
			task.Execute(ctx)
		case <-ctx.Done():

		}

	}
}

func (d *Dispatcher) Wait() {
	close(d.JobQueue)
	d.Wait()
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
}
