package main

import (
	"fmt"
	"sync"
	"time"
)

type ServiceType string

const (
	Deposit ServiceType = "Депозит"
	Credit  ServiceType = "Кредит"
)

type Client struct {
	ID      int
	Name    string
	Request ServiceType
}

type Specialist struct {
	ID         int
	Specialty  ServiceType
	Window     chan Client
	ReadyBoard chan chan Client
	quit       chan bool
	wg         *sync.WaitGroup
}

type BankManager struct {
	GlobalQueue chan Client
	Boards      map[ServiceType]chan chan Client
	Employees   []*Specialist
	wg          *sync.WaitGroup
}

func (s *Specialist) Work() {
	defer s.wg.Done()
	for {
		select {
		case s.ReadyBoard <- s.Window:
			client, ok := <-s.Window
			if !ok {
				return
			}
			fmt.Printf("[Клиент %d] обрабатывается специалистом %d\n", client.ID, s.ID)
			time.Sleep(1000 * time.Microsecond)
		case <-s.quit:
			return
		}
	}
}

func (m *BankManager) Run() {
	for _, employee := range m.Employees {
		m.wg.Add(1)
		go employee.Work()
	}

	go func() {
		for client := range m.GlobalQueue {
			window := <-m.Boards[client.Request]
			window <- client
		}

		for _, employee := range m.Employees {
			close(employee.quit)
		}
	}()
}

func CreateManager() *BankManager {
	m := &BankManager{
		GlobalQueue: make(chan Client),
		Boards:      make(map[ServiceType]chan chan Client),
		wg:          &sync.WaitGroup{},
	}

	if _, ok := m.Boards[Deposit]; !ok {
		m.Boards[Deposit] = make(chan chan Client)
	}

	if _, ok := m.Boards[Credit]; !ok {
		m.Boards[Credit] = make(chan chan Client)
	}

	return m
}

func CreateSpecialist(ID int, queue chan chan Client, wg *sync.WaitGroup) *Specialist {
	s := &Specialist{
		ID: ID,
		Window:     make(chan Client),
		ReadyBoard: queue,
		wg:         wg,
		quit:       make(chan bool),
	}

	return s
}

func main() {
	manager := CreateManager()
	for i := range 5 {
		var serviceType ServiceType
		if i%2 == 0 {
			serviceType = Deposit
		} else {
			serviceType = Credit
		}

		specialist := CreateSpecialist(i, manager.Boards[serviceType], manager.wg)
		manager.Employees = append(manager.Employees, specialist)

		fmt.Println(specialist)
		fmt.Println(manager)
	}

	manager.Run()

	for i := range 15 {
		var serviceType ServiceType
		if i%2 == 0 {
			serviceType = Deposit
		} else {
			serviceType = Credit
		}
		client := Client{ID: i, Name: fmt.Sprintf("Клиент %d", i), Request: serviceType}
		manager.GlobalQueue <- client
	}
	close(manager.GlobalQueue)
	manager.wg.Wait()
}
