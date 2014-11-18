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
	//"strconv"
	"strings"
	"time"
)

const SIZE = 100

var CHATTY int = 2

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
	slot       []Slot
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
	chat(1, fmt.Sprintf("I have been asked to accept Seq: [%d]\nCompare: [%d]\n", commandedSeqNum.N, lastLocalSeq.Cmp(commandedSeqNum)))
	if commandedSeqNum.Cmp(lastLocalSeq) < 1 {
		/*slot.Accepted = true
		slot.Decided = true
		slot.N.N = commandedSeqNum
		slot.N.Address = n.address
		slot.Data = in
		n.placeInSlot(slot, commandedSeqNum)*/
		//set the return, since it was successful we do not need to
		//send back the command
		chat(1, "I can accept\n")
		tempreply.Okay = true
		tempreply.Promised = lastLocalSeq
		n.recentSlot = in.Accepted.Slot
	} else {
		//the value was not placed
		//inform the sender and return the
		//command that we have in this slot
		chat(1, "I can NOT accept\n")
		tempreply.Okay = false
		tempreply.Promised = n.recentSlot.N
	}
	*reply = tempreply
	return nil
}

//just gathering majority quorum, does not need the data, just a slot and sequence number
func (n Node) Prepare(proposal Request, reply *PResponse) error {
	tempreply := *reply
	slotToWork := n.getSlot(proposal.Promise.Slot.Position)
	currentSeq := slotToWork.N
	proposedSeq := proposal.Promise.Sequence

	//was this slot decided or did I already promise a higher sequence number?
	// TODO: make a tie breaker. Checks greater than. If they are equal then use IP as tie-breaker
	// ^^^^^ finished. see func seq.cmp
	chat(1, fmt.Sprintf("Prepare Current Seq = [%d]\nProposed = [%d]\n", currentSeq.N, proposedSeq.N))
	if currentSeq.Cmp(proposedSeq) > -1 {
		tempreply.Okay = false
		chat(1, fmt.Sprintf("[%d] already promised to ----> [%s]", proposal.Promise.Slot.Position, slotToWork.N.Address))
		tempreply.Promised = proposedSeq
		tempreply.Command = slotToWork.Data
	} else {
		chat(1, fmt.Sprintf("Successful Prepare round: SeqN[%d]", proposedSeq.N))
		//slotToWork.Accepted = true
		slotToWork.N = proposedSeq
		//place the new sequence number and
		//accepted into a slot.
		n.placeInSlot(slotToWork, proposal.Promise.Slot.Position)

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
	chat(1, fmt.Sprintf("I have been asked to decide on: [%d]\nSlot [%d] Decided already?: [%t]\n", in.Decision.Slot.N.N, reqSlotPosition, localSlot.Decided))
	if localSlot.Decided && !localSlot.Data.SameCommand(in.Command) {
		chat(2, fmt.Sprintf("\n    Failed in decide:  \n    [%d] already has ------> [%s] ", in.Decision.Slot.Position, localSlot.Data.Print()))
		return nil
	}
	//quit if already decided
	if localSlot.Decided {
		chat(2, fmt.Sprintf("Failed in decide:  Slot [%d] from [%s] already decided ", in.Decision.Slot.Position, localSlot.N.Address))
		return nil
	}

	cmd := in.Decision.Value
	in.Decision.Slot.Decided = true
	elt.placeInSlot(in.Decision.Slot, in.Decision.Slot.Position)
	chat(1, fmt.Sprintf("Placed Slot#[%d] ---> Seq#[%d]", elt.getSlot(in.Decision.Slot.Position).Position, elt.getSlot(in.Decision.Slot.Position).N.N))
	//wait if everyone is not decided.
	//I got help with the alldecided function
	for !allDecided(elt.slot, in.Decision.Slot.Position) {
		time.Sleep(time.Second)
	}
	returnValue := elt.runCommand(cmd)

	if _, okay := elt.Acks[in.Decision.Value.Id]; okay {
		//I got help with this section
		// if found send the result across the channel, then remove the channel from the map and throw it away
		elt.Acks[in.Decision.Value.Id] <- returnValue
	}

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
		chat(1, "Setting Slot to 1 in Propose")
		slotToWork.N.Address = elt.address
		slotToWork.N.N = 1
	}
	//set our newly created slot to have the inputed commands and sequence
	slotToWork.Data = requestedCmd
	slotToWork.Data.SeqN = slotToWork.N

breakCount:
	for {
		time.Sleep(time.Second * 2)
		// pick first undecided
		for i := 1; i < len(elt.slot); i++ {
			index := i
			value := elt.getSlot(index)
			chat(0, fmt.Sprintf("Ranging Over slots: [%d] decided --> [%t]", index, value.Decided))
			if !value.Decided {
				//this will be where we propse to put this.
				slotToWork.Position = index
				chat(1, fmt.Sprintf("Picked Slot: %d\n", slotToWork.Position))
				break
			}
		}
		accept.Slot = slotToWork

		chat(3, fmt.Sprintf("\n     Propose: round %d\n     Slot: %d \n     Sequence: %d\n", count, accept.Slot.Position, accept.Slot.N.N))

		// prepare to the entire quorum
		responses := make(chan PResponse, len(elt.q))
		for _, address := range elt.q {
			chat(0, fmt.Sprintf("Running prepare on [%s]\n", address))
			go func(address string, sToWork Slot, sequence Seq, responses chan PResponse) {
				p := Promise{
					Slot:     sToWork,
					Sequence: sequence,
				}
				message := Request{
					Address: elt.address,
					Promise: p,
				}

				pReply := PResponse{}

				if err := elt.call(address, "Node.Prepare", message, &pReply); err != nil {
					chat(0, fmt.Sprintf("Connection failure during prepare ----> [%s]", address))
				}
				// put reponses into the channel
				responses <- pReply

			}(address, slotToWork, slotToWork.N, responses)
		}
		chat(1, "Count responses\n")
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
			chat(1, fmt.Sprintf("Response from node.prepare was [%t]", response.Okay))
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
				chat(1, fmt.Sprintf("Higher command recieved from : [ %s ]\nCommand: %t\n", responseCommand.SeqN.Address, responseCommand.Print()))
				mostRecentCommand = responseCommand
			}

			// optimize by leaving if this is a majority
			if elt.majority(repliedYes, repliedNo) != 0 {
				break
			}
		}

		// If this was voted yes
		if elt.majority(repliedYes, repliedNo) != 0 {

			// did anyone reply with a value? pick your own if no
			chat(1, "Majority achieved votes")
			chat(1, mostRecentCommand.Print())
			accept.Data = slotToWork.Data

			// Set sequence to the seq of the command
			accept.Sequence = slotToWork.N

			// If one or more of those replicas that voted for you have already ACCEPTED a value,
			// you should pick the highest-numbered value from among them, i.e. the one that was accepted with the highest n value.

			if mostRecentCommand.Id != "" && mostRecentCommand.Id != promisedValues.Command.Id {
				chat(2, fmt.Sprintf("\n     %s:\n     Already proposed %s", mostRecentCommand.SeqN.Address, mostRecentCommand.Print()))
				accept.Data = mostRecentCommand
				accept.Slot.Data = mostRecentCommand
				accept.Sequence = mostRecentCommand.SeqN

				in.Accept = accept
				elt.call(elt.address, "Node.PAccept", in, &reply)
			} else {
				chat(1, "Breaking at break count")
				break breakCount
			}
		}
		// higher seq because we failed
		chat(1, fmt.Sprintf("Incrementing sequence in propose: [%d]\n", slotToWork.N.N))
		slotToWork.N.N = highestSeqN + 1

		// To pause, pick a random amount of time between, say, 5ms and 10ms. If you fail again, pick a random sleep time between 10ms and 20ms

		count++
	}

	in.Accept = accept
	elt.call(elt.address, "Node.PAccept", in, &reply)

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
	for _, address := range elt.q {
		go func(address string, slotToWork Slot, sequence Seq, command Command, responses chan PResponse) {
			a := Accept{
				Slot:     slotToWork,
				Sequence: sequence,
				Data:     command,
			}
			message := Request{
				Address:  elt.address,
				Accepted: a,
			}
			pReply := PResponse{}

			if err := elt.call(address, "Node.Accept", message, &pReply); err != nil {
				chat(2, fmt.Sprintf("Connection failure in PAccept ---->   [%s]\n", address))
				return
			}
			// Send the response over a channel, we can assume that a majority WILL respond
			responses <- pReply
		}(address, slotToWork, requestedSeq, requestedCmd, responses)
	}

	// Get responses from go routines
	repliedYes := 0
	repliedNo := 0
	highestSeqN := 0
	for _ = range elt.q {
		// pull from the channel response
		tempResponse := <-responses
		responseSeqN := tempResponse.Promised.N
		//resp{Command, Promised, Okay}
		if tempResponse.Okay {
			repliedYes++
		} else {
			repliedNo++
		}

		// make note of the highest n value that any replica returns to you
		if responseSeqN > highestSeqN {
			highestSeqN = responseSeqN
		}

		// If I have a majority
		if elt.majority(repliedYes, repliedNo) != 0 {
			break
		}
	}

	if elt.majority(repliedYes, repliedNo) == 1 {
		chat(2, "Majority in PAccept")
		for _, address := range elt.q {
			go func(address string, slotToWork Slot, command Command) {
				giveEmTheD := Decision{
					Slot:  slotToWork,
					Value: command,
				}
				message := Request{
					Address:  elt.address,
					Decision: giveEmTheD,
				}
				pReply := PResponse{}

				if err := elt.call(address, "Node.Decide", message, &pReply); err != nil {
					chat(2, fmt.Sprintf("Connection failure in PAccept calling Decide --->  [%s]\n", address))
					return
				}
			}(address, slotToWork, requestedCmd)
		}

		return nil
	}

	chat(2, "No majority in PAccept")
	requestedCmd.SeqN.N = highestSeqN + 1

	p := Promise{
		Command: requestedCmd,
	}
	newMessage := Request{
		Address: elt.address,
		Promise: p,
	}
	newPReply := PResponse{}

	if err := elt.call(elt.address, "Node.Propose", newMessage, &newPReply); err != nil {
		chat(2, "Failed to connect to myself in PAccept calling Propose")
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
		q:        make([]string, 0),
		slot:     make([]Slot, 100),
		database: make(map[string]string),
		Acks:     make(map[string]chan string),
	}
	/*tempSlot := Slot{}
	tempSlot.Decided = true
	tempSlot.Position = 0
	node.placeInSlot(tempSlot, 0)
	*/
	for i := 1; i < len(addrin); i++ {
		if appendLocalHost(addrin[i]) != "" {
			node.q = Extend(node.q, appendLocalHost(addrin[i]))
		}
	}
	node.address = node.q[0]
	node.create()
	m := map[string]func(string) error{
		"help":      node.help,
		"put":       node.put,
		"ping":      node.ping,
		"putrandom": node.putRandom,
		"get":       node.get,
		"delete":    node.nDelete,
		"chat":      node.chatLevel,
		"dump":      node.dump,
		"quit":      quit,
		"testpa":    node.testpa,
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
