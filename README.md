cluster-server
==============

A simple cluster server package for golang. The library uses ZeroMQ bindings for golang (https://github.com/pebbe/zmq4).


Install
-------

To install the package in your GOPATH just run the following command:

  `$ go get github.com/pforpallav/cluster-server`
  

Usage
-----

To use the library in your go-code import the package by adding this import line

  `import cluster "github.com/pforpallav/cluster-server"`
  
Now you can access the interfaces, structs and functions by through `cluster` namespace.


Documentation
-------------

#### func AddPeer
`func AddPeer(id int, config string) Server`

   id - Peer's id  
   config - .json file with configuration (sample file is config.json)  

Starts a new peer in the cluster. The peer listens to address corresponding to the id mentioned in the config file. Returns a Server object.

#### type Server
`type Server interface`

  `Pid() int` - Return self's Pid in the cluster.  
  `Peers() []int` - Return id's to all the peers in the cluster.  
  `Outbox() chan *Envelope` - Return the channel for outbox.  
  `Inbox() chan *Envelope` - Return the channel for inbox.  
  
An interface to access basic information of the server.
  
#### type Envelope
`type Envelope struct`

   `Pid int` - The Pid of the peer from/to which the message was recieved/sent.  
   `MsgId int64` - Unique message id.  
   `Msg interface{}` - Actual message.  
   

Sample Test
-----------
  `<src folder> $ go test`
  
Testing with five servers. Multiple messages sent between themselves. Check for count. Check for message content.
