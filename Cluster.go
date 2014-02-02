package cluster

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	//"strings"
)

import zmq "github.com/pebbe/zmq4"

const (
	BROADCAST = -1
)

type Envelope struct {
	// On the sender side, Pid identifies the receiving peer. If instead, Pid is
	// set to cluster.BROADCAST, the message is sent to all peers. On the receiver side, the
	// Id is always set to the original sender. If the Id is not found, the message is silently dropped
	Pid int

	// An id that globally and uniquely identifies the message, meant for duplicate detection at
	// higher levels. It is opaque to this package.
	MsgId int64

	// the actual message.
	Msg interface{}
}

type Server interface {
	// Id of this server
	Pid() int

	// array of other servers' ids in the same cluster
	Peers() []int

	// the channel to use to send messages to other peers
	// Note that there are no guarantees of message delivery, and messages
	// are silently dropped
	Outbox() chan *Envelope

	// the channel to receive messages from other peers.
	Inbox() chan *Envelope
}

//Inteface for messaging
type MsgHandler interface {
	Sender() int
	Receiver() int
}

type ServerBody struct {
	// Id of this server
	MyId int

	// Address of this server
	MyAdd string

	// Number of servers in cluster
	NumServers int

	// Array of other servers' address
	PeerAdds []string

	// Array of other servers' id
	PeerIds []int

	// Outbox channel
	OutChan chan *Envelope

	// Inbox channel
	InChan chan *Envelope

	// Waiting to hear from somebody
	RecvChan chan int

	// Waiting to complete sending data
	SendChan chan int
}

//ServerBody implementation for Pid()
func (s ServerBody) Pid() int {
	return s.MyId
}

//ServerBody implementation for Peers()
func (s ServerBody) Peers() []int {
	return s.PeerIds
}

//ServerBody implementation for Outbox()
func (s ServerBody) Outbox() chan *Envelope {
	return s.OutChan
}

//ServerBody implementation for Inbox()
func (s ServerBody) Inbox() chan *Envelope {
	return s.InChan
}

//ServerBody implementation for Sender
func (s ServerBody) Sender() int {
	for {
		//Waiting for SendChannel to get free
		<-s.SendChan

		//Waiting for Outbox entry
		e := <-s.Outbox()

		//Changing the Pid to Sender
		var toId int
		toId = e.Pid
		e.Pid = s.Pid()
		m, err := json.Marshal(e) //Marshal encoding
		if err != nil {
			log.Fatal(err)
		}

		for j, toPid := range s.Peers() {
			if (toPid == toId || toId == -1) && toPid != s.MyId {
				//fmt.Printf("Sending Message to %s ...", s.PeerAdds[j])

				//context, err := zmq.NewContext()
				//if(err != nil) { log.Fatal(err) }

				socket, err := zmq.NewSocket(zmq.PUSH)
				if err != nil {
					log.Fatal(err)
				}

				err = socket.Connect(s.PeerAdds[j])
				if err != nil {
					log.Fatal(err)
				}
				//println("Connected")

				_, err = socket.SendBytes([]byte(m), 0)
				if err != nil {
					log.Fatal(err)
				}
				//println("Sent")

				socket.Close()
				//fmt.Printf(" Done\n")

				if toId != -1 {
					break
				}
			}
		}
		s.SendChan <- 1
	}

	return 0
}

//ServerBody implementation for Receiver
func (s ServerBody) Receiver() int {

	socket, _ := zmq.NewSocket(zmq.PULL)
	socket.Bind(s.MyAdd)

	//println("Bound to ",s.MyAdd)
	for {
		//Waiting on RecvChannel to get free
		<-s.RecvChan
		//println("Recieving")

		msg, err := socket.RecvBytes(0)
		if err != nil {
			log.Fatal(err)
		}
		//println("Received!")

		//Unmarshal the received message into an Envelope
		var e Envelope
		err = json.Unmarshal(msg, &e)
		if err != nil {
			log.Fatal(err)
		}

		//Sending on the Inbox channel
		s.Inbox() <- &e

		//Enabling next Recieve action
		s.RecvChan <- 1
	}

	return 0
}

func AddPeer(id int, config string) Server {

	//Struct for handling ConfigData
	type ConfigData struct {
		Total int      //Total number of servers
		Ids   []int    //All the ids
		Adds  []string //All the addresses (correspondingly)
	}

	ConfigFile, err := ioutil.ReadFile(config)
	if err != nil {
		panic(err)
	}

	//Decoding into a ConfigData
	var c ConfigData
	err = json.Unmarshal(ConfigFile, &c)
	if err != nil {
		panic(err)
	}

	var Me Server
	var MyStruct ServerBody

	for i, pid := range c.Ids {
		if pid == id {
			//Initialising Server
			MyStruct = ServerBody{pid, c.Adds[i], c.Total, c.Adds /*append(c.Adds[:i], c.Adds[i+1:]...)*/, c.Ids /*append(c.Ids[:i], c.Ids[i+1:]...)*/, make(chan *Envelope), make(chan *Envelope), make(chan int, 1), make(chan int, 1)}

			fmt.Printf("Starting peer %d at %s ...", id, c.Adds[i])
			//println(c.Adds[0])

			//Enabling Sender and Receiver channels
			MyStruct.RecvChan <- 1
			MyStruct.SendChan <- 1

			go MyStruct.Receiver()
			go MyStruct.Sender()
			fmt.Printf(" Server deployed\n")

			break
		}
	}

	Me = Server(MyStruct)

	return Me
}
