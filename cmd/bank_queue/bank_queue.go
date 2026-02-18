package main

import (
	"fmt"
	"sync"
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
	wg         sync.WaitGroup
}

type BankManager struct {
	GlobalQueue chan Client
	Boards      map[ServiceType]chan chan Client
	Employees   []*Specialist
	wg          sync.WaitGroup
}

func (s *Specialist) Work() {
	for {
		s.ReadyBoard <- s.Window

		select {
		case client := <- s.Window:
			fmt.Printf("[Клиент %d] обрабатывается", client.ID)
		case <-s.quit:
			return	
		}
	}
}

func (m *BankManager) Run() {
	go func() {
		for _, employee := range m.Employees {
			employee.Work()
		}
	}()

	go func() {
		for range 15 {
			client := <-m.GlobalQueue
			fmt.Println(client)
		}
	}()
}

func CreateManager() *BankManager {
	m := &BankManager{}
	m.GlobalQueue = make(chan Client)

	m.Boards = make(map[ServiceType]chan chan Client)

	if _, ok := m.Boards[Deposit]; !ok {
		m.Boards[Deposit] = make(chan chan Client)
	}

	if _, ok := m.Boards[Credit]; !ok {
		m.Boards[Credit] = make(chan chan Client)
	}

	return m
}

func CreateSpecialist(ID int, queue chan chan Client) *Specialist {
	s := &Specialist{
		Window:     make(chan Client),
		ReadyBoard: queue,
	}

	return s
}

func main() {
	manager := CreateManager()
	manager.Employees = make([]*Specialist, 5)
	for i := range 5 {
		var serviceType ServiceType
		if i%2 == 0 {
			serviceType = Deposit
		} else {
			serviceType = Credit
		}

		specialist := CreateSpecialist(i, manager.Boards[serviceType])
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
		client := Client{ID: 1, Name: fmt.Sprintf("Клиент %d", i), Request: serviceType}
		manager.GlobalQueue <- client
	}
}
