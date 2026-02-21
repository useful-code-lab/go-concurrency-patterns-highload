package main

import (
	"context"
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
	return nil
}

type DataTask struct {
	ID     int
	Result string
}

func (d *DataTask) Execute(ctx context.Context) error {
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

func (d *Dispatcher) Start() {

}

func (d *Dispatcher) Wait() {
	close(d.JobQueue)
	d.Wait()
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

}
