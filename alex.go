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
	SeqN    int
	Address string
}
type Proposal struct {
	Slot int
	N    Seq
}
type Node struct {
	address  string
	q        []string //quorum
	ledger   map[int]string
	database map[string]string
}
type Promise struct {
}
type PResponse struct {
	Okay     bool
	Promised int
	Command  string
}

func (n Node) Vote(line string, reply *string) error {

	return nil
}
func (n Node) Prepare(pro Proposal, response *PResponse) error {
	//sender := pro.N.Address
	//senderSlot := pro.Slot
	//senderSeq := pro.N.SeqN

	return nil
}
func (n Node) prepareRequest(slot, sequence int) (count int, promise int, command string) {
	promises := make(chan *PResponse, len(n.q))
	proposal := Proposal{
		Slot: slot,
		N:    Seq{sequence, n.address},
	}
	for _, replica := range n.q {
		log.Printf(replica)
		go func(address string) {
			response := new(PResponse)
			if err := n.call(address, "Node.Prepare", proposal, response); err != nil {
				chat(2, fmt.Sprintf("Error in calling Prepare on replica %s", address))
				promises <- nil
			} else {
				promises <- response
				log.Println(response.Command)
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

			if response.Promised > 0 {
				chat(1, fmt.Sprintf("[%d] Propose: --> \"yes\" vote recieved with accepted command={%s}", slot, command))
			} else {
				chat(1, fmt.Sprintf("[%d] Propose: --> \"yes\" vote recieved with no command", slot))
			}
		}
	}

	return 1, 1, "temp return"
}
func main() {
	addrin := os.Args
	var node = &Node{
		address:  "",
		q:        make([]string, 5),
		ledger:   make(map[int]string),
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
		"chat": node.chatLevel,
		"dump": node.dump,
		"quit": quit,
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
