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
type Command struct {
	SeqN  Seq
	Slot  int
	Id    int
	Type  string
	Key   string
	Value string
}
type Slot struct {
	Accepted bool
	Decided  bool
	N        Seq
	Data     Command
}
type Node struct {
	address  string
	q        []string //quorum
	slot     map[int]Slot
	database map[string]string
}
type PResponse struct {
	Okay     bool
	Promised Command
}
type Promise struct {
	SlotN    int
	Sequence Seq
}

func (n Node) Vote(line string, reply *string) error {

	return nil
}

//This actually needs the command
func (n Node) Accept(in Command, reply *PResponse) error {
	tempreply := *reply
	slot := n.getSlot(in.Slot)
	lastLocalSeq := slot.N.N
	commandedSeqNum := in.SeqN.N
	//accepted := slot.Accepted

	if lastLocalSeq <= commandedSeqNum {
		slot.Accepted = true
		slot.Decided = true
		slot.N.N = commandedSeqNum
		slot.N.Address = n.address
		slot.Data = in
		n.placeInSlot(slot, commandedSeqNum)
		//set the return, since it was successful we do not need to
		//send back the command
		tempreply.Okay = true
		tempreply.Promised = slot.Data
	} else {
		//the value was not placed
		//inform the sender and return the
		//command that we have in this slot
		tempreply.Okay = false
		tempreply.Promised = slot.Data
	}
	*reply = tempreply
	return nil
}

//just gathering majority quorum, does not need the data, just a slot and sequence number
func (n Node) Prepare(proposal Promise, reply *PResponse) error {
	tempreply := *reply
	slotToWork := n.getSlot(proposal.SlotN)
	currentSeq := slotToWork.N.N
	proposedSeq := proposal.Sequence.N

	//was this slot decided or did I already promise a higher sequence number?
	// TODO: make a tie breaker
	if currentSeq > proposedSeq {
		tempreply.Okay = false
		tempreply.Promised = slotToWork.Data
	} else {
		slotToWork.Accepted = true
		slotToWork.N.N = proposedSeq
		//place the new sequence number and
		//accepted into a slot.
		n.placeInSlot(slotToWork, proposal.SlotN)

		tempreply.Okay = true
		tempreply.Promised = slotToWork.Data
	}

	*reply = tempreply

	return nil
}

/*func (n Node) prepareRequest(slot, sequence int) (count int, promise int, command string) {
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
}*/
func main() {
	addrin := os.Args
	var node = &Node{
		address:  "",
		q:        make([]string, 5),
		slot:     make(map[int]Slot),
		database: make(map[string]string),
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
			//node.prepareRequest(1, 2)
		} else {
			str := strings.Fields(line)
			line = strings.Join(str[1:], " ")
			if _, ok := m[str[0]]; ok {
				m[str[0]](line)
			}
		}
	}
}
