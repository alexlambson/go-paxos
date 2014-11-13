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
	Slot     Slot
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
	slotToWork := n.getSlot(proposal.Slot.Position)
	currentSeq := slotToWork.N
	proposedSeq := proposal.Sequence

	//was this slot decided or did I already promise a higher sequence number?
	// TODO: make a tie breaker. Checks greater than. If they are equal then use IP as tie-breaker
	// ^^^^^ finished. see func seq.cmp
	if currentSeq.Cmp(proposedSeq) > -1 {
		tempreply.Okay = false
		chat(1, fmt.Sprintf("[%d]   already promised to ----> [%s]", proposal.Slot.Position, slotToWork.N.Address))
		tempreply.Promised = proposedSeq
	} else {
		//slotToWork.Accepted = true
		slotToWork.N = proposedSeq
		//place the new sequence number and
		//accepted into a slot.
		n.placeInSlot(slotToWork, proposal.Slot.Position)

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

func (elt Node) Propose(in Request, reply *PResponse) error {
	//tempreply := *reply
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
	slotToWork.Data = requestedCmd
	slotToWork.Data.SeqN = slotToWork.N

breakCount:
	for {
		// pick first undecided
		for index, value := range elt.slot {
			if !value.Decided {
				//this will be where we propse to put this.
				slotToWork.Position = index
				break
			}
		}
		accept.Slot = slotToWork

		chat(3, fmt.Sprintf("Propose: round %d\n     Slot: %d \n     Sequence: %d\n", count, slotToWork.Position, slotToWork.N.N))

		// prepare to the entire quorum
		responses := make(chan PResponse, len(elt.q))
		for _, address := range elt.q {
			go func(address string, slotToWork Slot, sequence Seq, responses chan PResponse) {
				p := Promise{
					Slot:     slotToWork,
					Sequence: sequence,
				}
				message := Request{
					Address: elt.address,
					Promise: p,
				}

				pReply := PResponse{}

				if err := elt.call(address, "Node.Prepare", message, &pReply); err != nil {
					chat(3, fmt.Sprintf("Connection failure during prepare ----> [%s]\n", address))
					return
				}
				// put reponses into the channel
				responses <- pReply

			}(address, slotToWork, slotToWork.N, responses)
		}
		// count responses
		repliedYes := 0
		repliedNo := 0
		highestSeqN := 0
		//blank command in case anyone replies with a higher one.
		mostRecentCommand := Command{}

		for _ = range elt.q {
			// pull from the channel response
			response := <-responses
			responseSeqN := response.Promised.N
			responseCommand := response.Command
			//count the replies
			if response.Okay {
				repliedYes++
			} else {
				repliedNo++
			}

			// do they have a higher sequence number than me?
			if responseSeqN > highestSeqN {
				highestSeqN = responseSeqN
			}
			// does a command have a higher sequence number than the previous champion?
			if responseCommand.SeqN.Cmp(mostRecentCommand.SeqN) > 0 {
				mostRecentCommand = responseCommand
			}

			// optimize by leaving if this is a majority
			if elt.majority(repliedYes, repliedNo) != 0 {
				break
			}
		}

		// If this was voted yes
		if elt.majority(repliedYes, repliedNo) == 1 {

			// did anyone reply with a value? pick your own if no

			accept.Data = slotToWork.Data

			// Set sequence to the seq of the command
			accept.Sequence = slotToWork.N

			// If one or more of those replicas that voted for you have already ACCEPTED a value,
			// you should pick the highest-numbered value from among them, i.e. the one that was accepted with the highest n value.

			if mostRecentCommand.Id != "" && mostRecentCommand.Id != promisedValues.Command.Id {
				accept.Data = mostRecentCommand
				accept.Slot.Data = mostRecentCommand

				in.Accept = accept
				elt.call(elt.address, "Node.Accept", in, &reply)
			} else {
				break breakCount
			}
		}
		// higher seq because we failed
		slotToWork.N.N = highestSeqN + 1

		// To pause, pick a random amount of time between, say, 5ms and 10ms. If you fail again, pick a random sleep time between 10ms and 20ms

		count++
	}

	in.Accept = accept
	elt.call(elt.address, "Node.Accept", in, &reply)

	return nil
}
func (elt Node) PAccept(in Request, reply *PResponse) error {
	//args
	requestedValues := in.Accept
	//aSlot
	slotToWork := requestedValues.Slot
	//aV
	requestedCmd := requestedValues.Data
	//aN
	requestedSeq := requestedValues.Sequence

	// ask everyone to accept
	//wow, this looks familiar
	responses := make(chan PResponse, len(elt.q))
	for _, v := range self.Friends {
		go func(v string, slot Slot, sequence Sequence, command Command, response chan Response) {
			req := Request{Address: self.Address, Accepted: Accepted{Slot: slot, Seq: sequence, Command: command}}
			var resp Response
			err := call(v, "Accepted", req, &resp)
			if err != nil {
				failure("Accepted (from Accept)")
				return
			}
			// Send the response over a channel, we can assume that a majority WILL respond
			response <- resp
		}(v, aSlot, aN, aV, response)
	}

	// Get responses from go routines
	numYes := 0
	numNo := 0
	highestN := 0
	for numVotes := 0; numVotes < len(self.Friends); numVotes++ {
		// pull from the channel response
		prepareResponse := <-response
		//resp{Command, Promised, Okay}
		if prepareResponse.Okay {
			numYes++
		} else {
			numNo++
		}

		// make note of the highest n value that any replica returns to you
		if prepareResponse.Promised.N > highestN {
			highestN = prepareResponse.Promised.N
		}

		// If I have a majority
		if numYes >= majority(len(self.Friends)) || numNo >= majority(len(self.Friends)) {
			break
		}
	}

	if numYes >= majority(len(self.Friends)) {
		logM("***             Received majority")
		for _, v := range self.Friends {
			go func(v string, slot Slot, command Command) {
				req := Request{Address: self.Address, Decide: Decide{Slot: slot, Value: command}}
				var resp Response
				err := call(v, "Decide", req, &resp)
				if err != nil {
					failure("Decide (from Accept)")
					return
				}
			}(v, aSlot, aV)
		}

		return nil
	}

	logM("***     Accept: NO majority")
	aV.Sequence.N = highestN + 1

	// To pause, pick a random amount of time between, say, 5ms and 10ms. If you fail again, pick a random sleep time between 10ms and 20ms
	duration := float64(5)
	offset := float64(duration) * rand.Float64()
	time.Sleep(time.Millisecond * time.Duration(duration+offset))

	req1 := Request{Address: self.Address, Propose: Propose{Command: aV}}
	var resp1 Response
	err := call(self.Address, "Propose", req1, &resp1)
	if err != nil {
		failure("Propose")
		return err
	}

	return nil
}

//

//

// main area

//

//
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
