package cluster

//import cluster "github.com/pforpallav/cluster-server"

import (
	"fmt"
	"testing"
	"time"
	//"encoding/json"
	//"strings"
)

const (
	NUMPEERS = 5
	MAINLOOP = 100
)

var WaitSender chan int
var WaitReceiver chan int
var WaitReceivers [NUMPEERS]chan int

// gothread for sending messages
func Sender(PeerArray [NUMPEERS]Server, t *testing.T) {
	
	for i := 0; i < MAINLOOP; i++ {
		for j := 1; j <= NUMPEERS; j++ {

			for k := 1; k <= NUMPEERS; k++ {

				if k == j {
					continue
				}

				msg := string(string(j) + " to " + string(k))
				PeerArray[j-1].Outbox() <- &Envelope{Pid: k, Msg: msg}
			}
		}
		<-time.After(20000 * time.Nanosecond)
	}
	WaitSender <- 1

}

// gothread for checking inboxes
func Receiver(PeerArray [NUMPEERS]Server, t *testing.T) {

	for i := 1; i <= NUMPEERS; i++ {

		//fmt.Printf("Waiting %d\n", i)
		go func(serverID int) {
			for j := 0; j < (NUMPEERS-1)*MAINLOOP; j++ {
				//fmt.Printf("Waiting %d", serverID)
				select {
				case e := <-PeerArray[serverID-1].Inbox():
					//fmt.Printf("Received msg from %d: '%s'\n", e.Pid, e.Msg)
					msg := string(string(e.Pid) + " to " + string(serverID))
					if e.Msg != msg {
						t.Error("Recieved message mismatch")
					}
				case <-time.After(10 * time.Second):
					t.Error("Timed out")
				}
			}
			//fmt.Printf("%d Done\n", i)
			WaitReceivers[serverID-1] <- 1

		}(i)

	}

	for k := 0; k < NUMPEERS; k++ {
		<-WaitReceivers[k]
	}
	WaitReceiver <- 1
}

func Broadcaster(PeerArray [NUMPEERS]Server, t *testing.T){

	for i := 0; i < MAINLOOP; i++ {
		for j := 1; j <= NUMPEERS; j++ {

			msg := string(string(j) + " to all")
			PeerArray[j-1].Outbox() <- &Envelope{Pid: BROADCAST, Msg: msg}
			<-time.After(60000 * time.Nanosecond)
		}
	}
	WaitSender <- 1

}

func BroadcastReceiver(PeerArray [NUMPEERS]Server, t *testing.T) {

	for i := 1; i <= NUMPEERS; i++ {

		//fmt.Printf("Waiting %d\n", i)
		go func(serverID int) {
			for j := 0; j < (NUMPEERS-1)*MAINLOOP; j++ {
				//fmt.Printf("Waiting %d", serverID)
				select {
				case e := <-PeerArray[serverID-1].Inbox():
					//fmt.Printf("Received msg from %d: '%s'\n", e.Pid, e.Msg)
					msg := string(string(e.Pid) + " to all")
					if e.Msg != msg {
						t.Error("Recieved message mismatch")
					}
				case <-time.After(10 * time.Second):
					t.Error("Timed out")
				}
			}
			//fmt.Printf("%d Done\n", i)
			WaitReceivers[serverID-1] <- 1

		}(i)

	}

	for k := 0; k < NUMPEERS; k++ {
		<-WaitReceivers[k]
	}
	WaitReceiver <- 1
}

func TestCluster(t *testing.T) {
	var PeerArray [NUMPEERS]Server
	for i := 1; i <= NUMPEERS; i++ {
		PeerArray[i-1] = AddPeer(i, "config.json")
	}

	fmt.Printf("All servers started!\nInitiating direct message testing.\n")

	WaitSender = make(chan int)
	WaitReceiver = make(chan int)

	for i := 0; i < NUMPEERS; i++ {
		WaitReceivers[i] = make(chan int)
	}

	go Sender(PeerArray, t)
	go Receiver(PeerArray, t)

	//wait on Sender and Receiver
	<-WaitSender
	<-WaitReceiver

	fmt.Printf("Initiating broadcast message testing.\n")

	go Broadcaster(PeerArray, t)
	go BroadcastReceiver(PeerArray, t)

	//wait on Broadcaster and BroadcastReceiver
	<-WaitSender
	<-WaitReceiver
}
