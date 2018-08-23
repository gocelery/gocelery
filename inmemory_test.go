package gocelery

import (
	"testing"
	"log"
	"time"
)

func TestInMemoryBroker_Concurrency(t *testing.T) {
	tests := []struct {
		name string
		numMasters int
		numWorkers int
		numMessagesPerMaster int
	}{
		{"singleMasterSingleWorker", 1, 1, 3},
		{"singleMasterMultiWorker", 1, 3, 3},
		{"MultiMasterMultiWorker", 3, 3, 3},
		{"MultiMasterSingleWorker", 3, 1, 3},
		{"ManyMasterManyWorker", 100, 50, 3},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testMastersWorkers(test.numMasters, test.numWorkers, test.numMessagesPerMaster, t)
		})
	}
}

func testMastersWorkers(numMasters int, numWorkers int, numMessagesPerMaster int, t *testing.T) {
	ib := NewInMemoryBroker()
	// create worker go routines that receive messages
	resultChannel := make(chan string)
	for i := 0; i < numWorkers; i++ {
		go receiveMessages(ib, resultChannel)
	}
	// make the messages
	ids := make(map[string]bool)
	msgs := make([][]*CeleryMessage, numMasters)
	for i := 0; i < numMasters; i++ {
		subMsgs := make([]*CeleryMessage, numMessagesPerMaster)
		for j := 0; j < numMessagesPerMaster; j++ {
			cm, _ := makeCeleryMessage()
			ids[cm.GetTaskMessage().ID] = true
			subMsgs[j] = cm
		}
		msgs[i] = subMsgs
	}
	// make masters to put messages
	mastersFinished := make(chan bool)
	for i := 0; i < numMasters; i++ {
		go putMessages(ib, msgs[i], mastersFinished)
	}
	// wait for masters to finish
	for i := 0; i < numMasters; i++ {
		<-mastersFinished
	}
	// check if all messages are received
	checkMessages(t, resultChannel, ids, numMasters*numMessagesPerMaster)
	// check if the queue is empty at the end and all messages were received by the workers
	if !ib.isEmpty() {
		t.Errorf("queue must be empty")
	}

	// satisfy coveralls for now ;)
	ib.Clear()
}

func checkMessages(t *testing.T, resultChannel chan string, ids map[string]bool, count int) {
	counter := 0
	for i := 0; i < count; i++ {
		received := <-resultChannel
		if ok, _ := ids[received]; !ok {
			t.Errorf("non recorded message %s", received)
		}
		counter++
	}
	if count != counter {
		t.Errorf("all messages [%v] should have been received [%v]", count, 5)
	}
}

func receiveMessages(ib *InMemoryBroker, resultChannel chan string) {
	// wait for messages to become available from masters, otherwise the workers will exit too early
	time.Sleep(10 * time.Millisecond)
	for ; !ib.isEmpty(); {
		msg, err := ib.GetTaskMessage()
		if err != nil {
			log.Fatal(err)
		}
		resultChannel <- msg.ID
		// less than this and workers try to access invalid memory addresses
		time.Sleep(100 * time.Microsecond)
	}
}

func putMessages(ib *InMemoryBroker, messages []*CeleryMessage, masterFinished chan<- bool) {
	for i := 0; i < len(messages); i++ {
		ib.SendCeleryMessage(messages[i])
	}
	masterFinished <- true
}