package main

import (
	"bufio"
	//"crypto/rand"
	"fmt"
	"log"
	//"math"
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
	Id    string
	Type  string
	Key   string
	Value string
}
type Slot struct {
	Decided  bool
	N        Seq
	Data     Command
	Position int
}
type Node struct {
	address  string
	q        []string //quorum
	slot     map[int]Slot
	database map[string]string
}
type PResponse struct {
	Okay     bool
	Promised Seq
	Command  Command
}
type Decision struct {
	Slot  Slot
	Value Command
}

//prepare
type Promise struct {
	SlotN    int
	Sequence Seq
	Command  Command
	Message  string
}

//send and recieve
type Accept struct {
	Slot     Slot
	Sequence Seq
	Data     Command
}
type Request struct {
	Promise  Promise
	Accepted Accept
	Command  Command
	Accept   Accept
	Decision Decision
	Message  string
	Address  string
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
	currentSeq := slotToWork.N
	proposedSeq := proposal.Sequence

	//was this slot decided or did I already promise a higher sequence number?
	// TODO: make a tie breaker. Checks greater than. If they are equal then use IP as tie-breaker
	// ^^^^^ finished. see func seq.cmp
	if currentSeq.Cmp(proposedSeq) > -1 {
		tempreply.Okay = false
		chat(1, fmt.Sprintf("[%d]   already promised to ----> [%s]", proposal.SlotN, slotToWork.N.Address))
		tempreply.Promised = proposedSeq
	} else {
		//slotToWork.Accepted = true
		slotToWork.N = proposedSeq
		//place the new sequence number and
		//accepted into a slot.
		n.placeInSlot(slotToWork, proposal.SlotN)

		tempreply.Okay = true
		tempreply.Promised = proposedSeq
		//will be blank if slot did not exist
		tempreply.Command = slotToWork.Data

	}

	*reply = tempreply

	return nil
}
func (elt Node) Decide(in Decision, reply *bool) error {

	// sleep
	duration := float64(latency)
	offset := float64(duration) * rand.Float64()
	time.Sleep(time.Second * time.Duration(duration+offset))

	args := req.Decide
	//logM("")
	logM("****  Decide:   " + args.Slot.String())

	if self.Slots[args.Slot.N].Decided && self.Slots[args.Slot.N].Command.Command != args.Value.Command {
		logM("****  Decide:  Already decided slot " + strconv.Itoa(args.Slot.N) + " with different command " + args.Value.Command)
		failure("Decide")
		return nil
	}
	// If already decided, quit
	if self.Slots[args.Slot.N].Decided {
		logM("****  Decide:  Already decided slot " + strconv.Itoa(args.Slot.N) + " with command " + args.Value.Command)
		return nil
	}

	_, ok := self.Acks[args.Value.Key]
	if ok {
		// if found send the result across the channel, then remove the channel from the map and throw it away
		self.Acks[args.Value.Key] <- args.Value.Command
	}

	command := strings.Split(args.Value.Command, " ")
	args.Slot.Decided = true
	self.Slots[args.Slot.N] = args.Slot

	for !allDecided(self.Slots, args.Slot.N) {
		time.Sleep(time.Second)
	}
	switch command[0] {
	case "put":
		logM("")
		logM("COMMAND COMPLETE- Put " + command[1] + " " + command[2])
		self.Database[command[1]] = command[2]
		break
	case "get":
		logM("")
		logM("COMMAND COMPLETE- Get ----> Key: " + command[1] + ", Value: " + self.Database[command[1]])
		break
	case "delete":
		logM("")
		logM("COMMAND COMPLETE- " + command[1] + " deleted.")
		delete(self.Database, command[1])
		break
	}
	self.Current++

	return nil
}

func (n Node) prepareRequest(slotNum, sequence int) (count int, highest int, command Command) {
	promises := make(chan *PResponse, len(n.q))
	prepareRequest := Promise{
		SlotN:    slotNum,
		Sequence: Seq{sequence, n.address},
	}
	for _, replica := range n.assemble() {
		chat(1, replica)
		go func(address string) {
			response := new(PResponse)
			if err := n.call(address, "Node.Prepare", prepareRequest, response); err != nil {
				chat(2, fmt.Sprintf("Error in calling Prepare on replica %s", address))
				promises <- nil
			} else {
				promises <- response
				log.Println(response.Promised)
			}
		}(replica)
	}

	count = 0
	highest = 0
	votedNo := 0

	for _ = range n.q {
		response := <-promises
		switch {
		case response == nil:
			votedNo++
			chat(1, "Empty response, counting it as no")
		case response.Okay:
			count++
			highest++
			if response.Promised.SeqN.N > 0 {
				chat(1, fmt.Sprintf("[%d] ---> \"yes\" on command   {%s}  ", slotNum, command))
			} else {
				chat(1, fmt.Sprintf("[%d] ---> \"yes\" no value", slotNum))
			}

			// see if a response is a higher sequence number and record its value
			if response.Promised.SeqN.Cmp(command.SeqN) {
				command = response.Promised
			}
			//record the highest sequence number. This is for when we need to execute this again
			if response.Promised.SeqN.N > highest {
				highest = response.Promised.SeqN.N
			}
		case !response.Okay:
			votedNo++
			if response.Promised.SeqN.N > highest {
				highest = response.Promised.SeqN.N
			}
		}
	}

	return count, highest, command
}

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
		"put":  node.parseTest,
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
