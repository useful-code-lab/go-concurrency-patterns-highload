package main

import (
	"sync"
	"time"
)

type Task func()

type Pool struct {
	Tasks          chan Task //Задачи
	MaxWorkers     int       //Максимальное количество обработчиков
	MinWorkers     int       //Минимальное количество обработчиков
	CurrentWorkers int       //Количество ворекров сейчас
	Quit           chan bool //Канал для завершения

	mu sync.Mutex //Мьютекс
}

func (p *Pool) Worker(tasks <-chan Task) {
	p.mu.Lock()
	p.CurrentWorkers++
	p.mu.Unlock()

	for {
		func() {
			select {
			case task := <-tasks:
				task()
			case <-time.After(time.Second * 5):
				if p.CurrentWorkers > p.MinWorkers {
					p.mu.Lock()
					p.CurrentWorkers--
					p.mu.Unlock()
					return
				}
			case <-p.Quit:
				return
			}
		}()
	}
}

func (p *Pool) NewPool(tasks chan Task, maxWorkers int, minWorker int) {
	pool := &Pool{
		Tasks:      tasks,
		MaxWorkers: maxWorkers,
		MinWorkers: minWorker,
		Quit:       make(chan bool),
	}

	for i := 0; i <= p.MinWorkers; i++ {
		p.CurrentWorkers++
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
