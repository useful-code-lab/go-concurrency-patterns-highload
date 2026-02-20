package main

import (
	"errors"
	"fmt"
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
	pg sync.WaitGroup
}

func (p *Pool) Worker() {
	p.pg.Done()
	idleTimeout := time.Second * 4
	timer := time.NewTimer(idleTimeout)
	defer timer.Stop()

	for {
		select {
		case task, ok := <-p.Tasks:
			if !ok {
				return
			}
			task()
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(idleTimeout)

		case <-timer.C:
			p.mu.Lock()
			if p.CurrentWorkers > p.MinWorkers {
				p.CurrentWorkers--
				p.mu.Unlock()
				return
			}
			p.mu.Unlock()
			timer.Reset(idleTimeout)
		case <-p.Quit:
			return
		}
	}
}

func NewPool(tasks chan Task, maxWorkers int, minWorker int) *Pool {
	p := &Pool{
		Tasks:      tasks,
		MaxWorkers: maxWorkers,
		MinWorkers: minWorker,
		Quit:       make(chan bool),
	}

	for i := 0; i < p.MinWorkers; i++ {
		p.mu.Lock()
		p.CurrentWorkers++
		p.mu.Unlock()
		p.pg.Add(1)
		go p.Worker()
	}

	go p.CreateAutoTask()

	return p
}

func (p *Pool) CreateAutoTask() {
	for {
		time.Sleep(time.Millisecond * 10)
		p.mu.Lock()
		if len(p.Tasks) > 6 && p.CurrentWorkers < p.MaxWorkers {
			p.CurrentWorkers++
			p.pg.Add(1)
			go p.Worker()
		}
		p.mu.Unlock()
	}
}

func (p *Pool) Submit(task Task) error {
	select {
	case p.Tasks <- task: //Присваивание в select на случай переполнения канала
		return nil
	default:
		return errors.New("Переполнение канала") //Возвращаем ошибку если очередь забита
	}
}

func (p *Pool) Shutdown(){
	close(p.Tasks)
	close(p.Quit)
	p.pg.Wait()
}

func main() {
	tasks := make(chan Task, 100)
	pool := NewPool(tasks, 5, 2)

	for i := range 50 {
		pool.Submit(func() {
			fmt.Printf("Выполняется функция %d", i)
			time.Sleep(time.Second * 4)
		})
	}

	pool.Shutdown()
}
