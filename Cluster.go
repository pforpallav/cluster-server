package cluster

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	//"log"
	//"strings"
)

import zmq "github.com/alecthomas/gozmq"

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

type MsgHandler interface {
	SendTo(add string, msg interface{}) int
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

//ServerBody function for

func AddPeer(id int, config string) Server {

	type ConfigData struct {
		Total int
		Ids   []int
		Adds  []string
	}

	ConfigFile, err := ioutil.ReadFile(config)
	if err != nil {
		panic(err)
	}

	var c ConfigData
	err = json.Unmarshal(ConfigFile, &c)
	if err != nil {
		panic(err)
	}

	var Me Server
	var MyStruct ServerBody

	for i, pid := range c.Ids {
		if pid == id {
			MyStruct = ServerBody{pid, c.Adds[i], c.Total, c.Adds /*append(c.Adds[:i], c.Adds[i+1:]...)*/, c.Ids /*append(c.Ids[:i], c.Ids[i+1:]...)*/, make(chan *Envelope), make(chan *Envelope), make(chan int, 1), make(chan int, 1)}

			fmt.Printf("Starting peer %d at %s\n", id, c.Adds[i])
			//println(c.Adds[0])

			context, _ := zmq.NewContext()
			socket, _ := context.NewSocket(zmq.REP)
			socket.Bind(c.Adds[i])

			MyStruct.RecvChan <- 1
			MyStruct.SendChan <- 1

			go func() {
				for {
					<-MyStruct.RecvChan
					msg, _ := socket.Recv(0)

					fmt.Printf("Recvd something")

					var e Envelope

					err = json.Unmarshal(msg, &e)
					println(e.Msg)
					Me.Inbox() <- &e
					println("Message recieved")
					MyStruct.RecvChan <- 1
				}
			}()

			go func() {
				for {
					<-MyStruct.SendChan
					//println("Am here1")
					e := <-Me.Outbox()
					var toId int
					toId = e.Pid
					e.Pid = Me.Pid()
					m, _ := json.Marshal(e)

					for j, toPid := range Me.Peers() {
						if toPid == toId {
							fmt.Printf("Sending Message to %s", MyStruct.PeerAdds[j])
							socket.Connect(MyStruct.PeerAdds[j])
							socket.Send([]byte(m), 0)
							fmt.Printf(" Done\n")
							break
						}
					}
					MyStruct.SendChan <- 1
				}
			}()

			fmt.Printf("Server deployed\n")

			break
		}
	}

	Me = Server(MyStruct)

	return Me
}
