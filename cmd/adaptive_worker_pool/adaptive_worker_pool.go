package main

type Task struct {

}

type Pool struct {
	tasks chan Task
	maxWorkers int
	minWorkers int
	sem chan struct{}
	quit chan bool
}

func main() {

}