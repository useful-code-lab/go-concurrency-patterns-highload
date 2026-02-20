package main

import "fmt"

type Task func()

type Pool struct {
	Tasks      chan Task //Задачи
	MaxWorkers int //Максимальное количество обработчиков
	MinWorkers int //Минимальное количество обработчиков
	Sem        chan struct{} //Семафор
	Quit       chan bool //Канал для завершения
}

func (p *Pool) Worker(tasks <-chan Task) {
	for range p.tasks {
		fmt.Printf("Выполняются таски")
	}
}

func (p *Pool) NewPool(tasks chan Task, maxWorkers int, minWorker int) {
	pool := &Pool{
		Tasks: tasks,
		MaxWorkers: maxWorkers,
		MinWorkers: minWorker,
		Sem: make(chan struct{}),
		Quit: make(chan bool),
	}

	for i := 0; i <= p.MinWorkers; i++ {
		go p.Worker(tasks)
	}
}

func (p *Pool) Submit(task Task) error {
	select {
	case p.tasks <- task:
		return nil
	default:
		return ErrPoolFull //Возвращаем ошибку если очередь забита
	}
}

func IdleTimeout() {
	
}

func main() {

}
