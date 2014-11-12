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
	"time"
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
	address    string
	q          []string //quorum
	slot       map[int]Slot
	database   map[string]string
	recentSlot Slot
	Acks       map[string]chan string
	currentSeq int
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
func (n Node) Accept(in Request, reply *PResponse) error {
	tempreply := *reply
	slot := n.getSlot(in.Accepted.Slot.Position)
	lastLocalSeq := slot.N
	commandedSeqNum := in.Accepted.Sequence
	//accepted := slot.Accepted

	if lastLocalSeq.Cmp(commandedSeqNum) == 0 {
		/*slot.Accepted = true
		slot.Decided = true
		slot.N.N = commandedSeqNum
		slot.N.Address = n.address
		slot.Data = in
		n.placeInSlot(slot, commandedSeqNum)*/
		//set the return, since it was successful we do not need to
		//send back the command
		tempreply.Okay = true
		tempreply.Promised = lastLocalSeq
		n.recentSlot = in.Accepted.Slot
	} else {
		//the value was not placed
		//inform the sender and return the
		//command that we have in this slot
		tempreply.Okay = false
		tempreply.Promised = n.recentSlot.N
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
func (elt Node) Decide(in Request, reply *bool) error {

	reqSlotPosition := in.Decision.Slot.Position
	localSlot := elt.getSlot(reqSlotPosition)

	if localSlot.Decided && !localSlot.Data.SameCommand(in.Command) {
		chat(2, fmt.Sprintf("Failed in decide:  [%d] already has ------> [%s] ", in.Decision.Slot.Position, localSlot.Data.Print()))
		return nil
	}
	//quit if already decided
	if localSlot.Decided {
		chat(2, fmt.Sprintf("Failed in decide:  [%d] already has ------> %s ", in.Decision.Slot.Position, localSlot.Data.Print()))
		return nil
	}

	if _, okay := elt.Acks[in.Decision.Value.Key]; okay {
		//I got help with this section
		// if found send the result across the channel, then remove the channel from the map and throw it away
		elt.Acks[in.Decision.Value.Key] <- in.Decision.Value.Print()
	}

	cmd := in.Decision.Value
	in.Decision.Slot.Decided = true
	elt.placeInSlot(in.Decision.Slot, in.Decision.Slot.Position)

	//wait if everyone is not decided.
	//I got help with the alldecided function
	for !allDecided(elt.slot, in.Decision.Slot.Position) {
		time.Sleep(time.Second)
	}
	elt.runCommand(cmd)
	elt.currentSeq++

	return nil
}

/*func (n Node) prepareRequest(slotNum, sequence int) (count int, highest int, command Command) {
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
}*/

func (elt Node) Propose(in Request, reply *PResponse) error {

	tempreply := *reply
	promisedValues := in.Promise
	requestedCmd := promisedValues.Command
	requestedSeq := in.Promise.Command.SeqN

	slotToWork := Slot{}
	accept := Accept{}
	count := 1
	slotToWork.N = requestedSeq

	//is this the first one? Offset it then give it our own address
	if slotToWork.N.N == 0 {
		slotToWork.N.Address = elt.address
		slotToWork.N.N = 1
	}
	//set our newly created slot to have the inputed commands and sequence
	slotToWork.Command = requestedCmd
	slotToWork.Data.SeqN = slotToWork.N

	for {
		// pick first undecided
		for index, value := range elt.slot {
			if !value.Decided {
				//this will be where we propse to put this.
				slotToWork.Position = index
				break
			}
		}
		acceptance.Slot = pSlot

		logM("*     Propose:  Round: " + strconv.Itoa(round))
		logM("*               Slot: " + strconv.Itoa(pSlot.N))
		logM("*               N: " + strconv.Itoa(pSlot.Sequence.N))

		// Send a Prepare message out to the entire cell (using a go routine per replica)
		response := make(chan Response, len(self.Friends))
		for _, v := range self.Friends {
			go func(v string, slot Slot, sequence Sequence, response chan Response) {
				req := Request{Address: self.Address, Prepare: Prepare{Slot: slot, Seq: sequence}}
				var resp Response
				err := call(v, "Prepare", req, &resp)
				if err != nil {
					failure("Prepare (from Propose)")
					return
				}
				// Send the response over a channel, we can assume that a majority WILL respond
				response <- resp

			}(v, pSlot, pSlot.Sequence, response)
		}
		// Get responses from go routines
		numYes := 0
		numNo := 0
		highestN := 0
		var highestCommand Command
		for numVotes := 0; numVotes < len(self.Friends); numVotes++ {
			// pull from the channel response
			prepareResponse := <-response
			if prepareResponse.Okay {
				numYes++
			} else {
				numNo++
			}

			// make note of the highest n value that any replica returns to you
			if prepareResponse.Promised.N > highestN {
				highestN = prepareResponse.Promised.N
			}
			// track the highest-sequenced command that has already been accepted by one or more of the replicas
			if prepareResponse.Command.Sequence.N > highestCommand.Sequence.N {
				highestCommand = prepareResponse.Command
			}

			// If I have a majority
			if numYes >= majority(len(self.Friends)) || numNo >= majority(len(self.Friends)) {
				break
			}
		}

		// If I have a majority
		if numYes >= majority(len(self.Friends)) {
			// select your value
			// If none of the replicas that voted for you included a value, you can pick your own.
			acceptance.V = pSlot.Command

			// In either case, you should associate the value you are about to send out for acceptance with your promised n
			acceptance.N = pSlot.Sequence

			// If one or more of those replicas that voted for you have already ACCEPTED a value,
			// you should pick the highest-numbered value from among them, i.e. the one that was accepted with the highest n value.

			if highestCommand.Tag > 0 && highestCommand.Tag != args.Command.Tag {
				acceptance.V = highestCommand
				acceptance.Slot.Command = highestCommand

				req.Accept = acceptance
				call(self.Address, "Accept", req, resp)
			} else {
				break rounds
			}
		}
		// No Majority? Generate a larger n for the next round
		pSlot.Sequence.N = highestN + 1

		// To pause, pick a random amount of time between, say, 5ms and 10ms. If you fail again, pick a random sleep time between 10ms and 20ms
		duration := float64(5 * round)
		offset := float64(duration) * rand.Float64()
		time.Sleep(time.Millisecond * time.Duration(duration+offset))
		round++
	}

	req.Accept = acceptance
	call(self.Address, "Accept", req, resp)

	return nil
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
