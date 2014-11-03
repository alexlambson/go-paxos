package main

import (
	"bufio"
	//"crypto/rand"
	"fmt"
	"log"
	//"math/big"
	//"net"
	//"net/http"
	//"net/rpc"
	"os"
	"strconv"
	"strings"
	//"time"
)

const SIZE = 100

var CHATTY int = 3

type Seq struct {
	N       int
	Address string
}
type Proposal struct {
	Slot Promise
	N    Seq
}
type Command struct {
	SeqN Seq
	Slot Promise
	Id   int
}
type Node struct {
	address  string
	q        []string //quorum
	promised map[string]Promise
	lastSeq  int
	database map[string]string
}
type PResponse struct {
	Okay     bool
	Promised Promise
}
type Promise struct {
	Type     string
	Key      string
	Value    string
	Sequence Seq
}

func (n Node) Vote(line string, reply *string) error {

	return nil
}
func (n Node) Accept(in Command, reply *PResponse) error {
	tempreply := *reply
	if value, exists := n.promised[in.Slot.Key]; exists {
		if value.Sequence.N > in.SeqN.N {
			tempreply.Okay = false
			tempreply.Promised = value
		} else {
			n.promised[in.Slot.Key] = in.Slot
			tempreply.Okay = true
			tempreply.Promised = in.Slot
		}
	} else {
		n.promised[in.Slot.Key] = in.Slot
		tempreply.Okay = true
		tempreply.Promised = in.Slot
	}
	*reply = tempreply
	return nil
}
func (n Node) Prepare(proposal Promise, reply *PResponse) error {
	tempreply := *reply

	if proposal.Sequence.N > n.lastSeq {
		n.promised[proposal.Key] = proposal
		n.lastSeq = proposal.Sequence.N
		tempreply.Okay = true
		tempreply.Promised = proposal
	} else {
		tempreply.Okay = false
		tempreply.Promised = n.promised[proposal.Key]
	}
	*reply = tempreply

	return nil
}
func (n Node) prepareRequest(slot, sequence int) (count int, promise int, command string) {
	promises := make(chan *PResponse, len(n.q))
	proposal := Proposal{
		Slot: *new(Promise),
		N:    Seq{sequence, n.address},
	}
	for _, replica := range n.assemble() {
		log.Printf(replica)
		go func(address string) {
			response := new(PResponse)
			if err := n.call(address, "Node.Prepare", proposal, response); err != nil {
				chat(2, fmt.Sprintf("Error in calling Prepare on replica %s", address))
				promises <- nil
			} else {
				promises <- response
				log.Println(response.Promised)
			}
		}(replica)
	}

	count = 0
	promise = 0
	votedNo := 0

	for _ = range n.q {
		response := <-promises
		switch {
		case response == nil:
			votedNo++
			chat(1, "Empty response, counting it as no")
		case response.Okay:
			count++

		}
	}

	return 1, 1, "temp return"
}
func main() {
	addrin := os.Args
	var node = &Node{
		address:  "",
		q:        make([]string, 5),
		promised: make(map[string]Promise),
		database: make(map[string]string),
		lastSeq:  0,
	}
	if len(addrin) != 6 {
		for i := 0; i < 5; i++ {
			node.q[i] = appendLocalHost(":341" + strconv.Itoa(i))
		}
	} else {
		for i := 0; i < 5; i++ {
			node.q[i] = appendLocalHost(addrin[i+1])
		}
	}
	node.address = node.q[0]
	node.create()
	m := map[string]func(string) error{
		"help": node.help,
		//"put":       node.putRequest,
		//"putrandom": node.putRandom,
		//"get":    node.getRequest,
		//"delete": node.deleteRequest,
		"chat":   node.chatLevel,
		"dump":   node.dump,
		"quit":   quit,
		"testpa": node.testpa,
	}
	fmt.Println("Paxos Implementation by Alex and Colton")
	fmt.Println("Listening on:	", node.address)
	fmt.Println()
	for {
		reader := bufio.NewReader(os.Stdin)
		line, err := reader.ReadString('\n')
		line = strings.TrimSpace(line)
		if err != nil {
			log.Fatal("Can't read string!", err)
		}
		if line == "" {
			node.help("")
		} else if line == "test" {
			node.prepareRequest(1, 2)
		} else {
			str := strings.Fields(line)
			line = strings.Join(str[1:], " ")
			if _, ok := m[str[0]]; ok {
				m[str[0]](line)
			}
		}
	}
}
