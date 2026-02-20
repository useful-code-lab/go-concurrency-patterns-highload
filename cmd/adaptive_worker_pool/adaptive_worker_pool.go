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
}

func (p *Pool) Worker() {
	p.mu.Lock()
	p.CurrentWorkers++
	p.mu.Unlock()

	for {
		func() {
			select {
			case task := <-p.Tasks:
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

func NewPool(tasks chan Task, maxWorkers int, minWorker int) *Pool {
	p := &Pool{
		Tasks:      tasks,
		MaxWorkers: maxWorkers,
		MinWorkers: minWorker,
		Quit:       make(chan bool),
	}

	for i := 0; i <= p.MinWorkers; i++ {
		p.CurrentWorkers++
		go p.Worker()
	}

	go p.CreateAutoTask()

	return p
}

func (p *Pool) CreateAutoTask() {
	for {
		time.Sleep(time.Millisecond * 10)
		if len(p.Tasks) > 6 {
			go p.Worker()
		}
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

func main() {
	tasks := make(chan Task, 100)
	pool := NewPool(tasks, 5, 2)

	for i := range 50 {
		tasks <- func() {
			fmt.Printf("Выполняется функция %d", i)
			time.Sleep(time.Second * 4)
		}
	}

	pool.Quit <- true
}
