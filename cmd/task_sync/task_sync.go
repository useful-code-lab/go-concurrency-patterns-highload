package main

import "context"

type Task interface {
	Execute(ctx context.Context) error
}

type EmailTask struct {
}

func (e *EmailTask) Execute(ctx context.Context) error {
	return nil
}

type DataTask struct {

}

func (d *DataTask) Execute(ctx context.Context) error {
	return nil
}

func main() {

}
