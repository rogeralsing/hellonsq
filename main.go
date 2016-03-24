package main

import (
	"log"
	"github.com/bitly/go-nsq"
	"sync"
)

func SendMessage(config *nsq.Config){
	w, _ := nsq.NewProducer("127.0.0.1:4150", config)

	err := w.Publish("write_test", []byte("test"))
	if err != nil {
		log.Panic("Could not connect")
	}

	w.Stop()
}

func ReceiveMessage(config *nsq.Config){
	wg := &sync.WaitGroup{}
	wg.Add(1)

	q, _ := nsq.NewConsumer("write_test", "ch", config)
	q.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		log.Printf("Got a message: %v", message)
		wg.Done()
		return nil
	}))
	err := q.ConnectToNSQD("127.0.0.1:4150")
	if err != nil {
		log.Panic("Could not connect")
	}
	wg.Wait()
}

func main() {
	config := nsq.NewConfig()
	SendMessage(config)
	ReceiveMessage(config)
}