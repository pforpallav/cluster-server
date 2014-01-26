package cluster

//import cluster "github.com/pforpallav/cluster-server"

import (
  "fmt"
  "time"
  "testing"
  //"encoding/json"
  //"strings"
)

const (
	NUMPEERS = 5
	MAINLOOP = 100
)

var WaitSender chan int
var WaitReciever chan int

// gothread for sending messages
func Sender(PeerArray [NUMPEERS]Server, t *testing.T){
	for i := 0; i < MAINLOOP; i++ {
		for j := 1; j <= NUMPEERS; j++ {

			for k := 1; k <= NUMPEERS; k++ {
				
				if k == j {
					continue
				}

				msg := string(string(j) + " to " + string(k) + " " + string(i))
				PeerArray[j-1].Outbox() <- &Envelope{Pid: k, Msg: msg}
			}
		}
	}
	WaitSender <- 1
}

// gothread for checking inboxes
func Reciever(PeerArray [NUMPEERS]Server, t *testing.T){
	for i := 0; i < MAINLOOP; i++ {
		for j := 1; j <= NUMPEERS; j++ {

			for k := 1; k <= NUMPEERS; k++ {
				
				if k == j {
					continue
				}

				msg := string(string(j) + " to " + string(k) + " " + string(i))
				select {
					case e := <- PeerArray[k-1].Inbox():
						//fmt.Printf("Received msg from %d: '%s'\n", e.Pid, e.Msg)
						if(e.Msg != msg){
							t.Error("Recieved message mismatch")
						}
					case <- time.After(10 * time.Second):
						t.Error("Timed out")
				}
			}
		}
	}
	WaitReciever <- 1
}

func TestCluster(t *testing.T) {
	var PeerArray [NUMPEERS]Server
	for i := 1; i <= NUMPEERS; i++ {
		PeerArray[i-1] = AddPeer(i, "config.json")
	}

	fmt.Printf("All servers started!\nInitiating message transfers.\n\nTest Result: ")

	WaitSender = make(chan int)
	WaitReciever = make(chan int)

	go Sender(PeerArray, t)
	go Reciever(PeerArray, t)
	
	//wait on Sender and Reciever
	<-WaitSender
	<-WaitReciever
}