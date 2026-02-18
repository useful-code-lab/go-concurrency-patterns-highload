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
	Employee    []*Specialist
	wg          sync.WaitGroup
}

func (s *Specialist) Work() {

}

func (m *BankManager) Run() {

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
	s := &Specialist{}
	s.ReadyBoard = queue

	return s
}

func main() {
	manager := CreateManager()
	manager.Employee = make([]*Specialist, 5)
	for i := range 5 {
		var serviceType ServiceType
		if i%2 == 0 {
			serviceType = Deposit
		} else {
			serviceType = Credit
		}

		specialist := CreateSpecialist(i, manager.Boards[serviceType])
		manager.Employee = append(manager.Employee, specialist)

		fmt.Println(specialist)
		fmt.Println(manager)

	}
}
