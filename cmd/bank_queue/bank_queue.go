package main

import "sync"

type ServiceType string

const (
	Deposit ServiceType = "Депозит"
	Credit ServiceType = "Кредит"
)
type Client struct {
	ID int
	Name string
	Request ServiceType
}

type Specialist struct {
	ID int
	Specialty ServiceType
	Window chan Client
	ReadyBoard  chan chan Client
	quit chan bool
	wg sync.WaitGroup
}

type BankManager struct {
	GlobalQueue chan Client
	Boards map[ServiceType] chan chan Client
	Employee []Specialist
	wg sync.WaitGroup
}

func main() {
	
}