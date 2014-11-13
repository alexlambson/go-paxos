package main

import (
	//"bufio"
	"crypto/rand"
	//"flag"
	"fmt"
	"log"
	"math/big"
	//"math/rand"
	"errors"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	//"time"
	"crypto/sha1"
)

func (n *Node) help(line string) error {
	log.Print("      The list of commands are:      ")
	log.Print("  put <key>      , <value>, <address>")
	log.Print("  putrandom <n>  , get <key>         ")
	log.Print("  delete <key>   , dump              ")
	log.Print("	 get <key> 		, <value> <address> ")
	return nil
}
func (n *Node) dump(_ string) error {
	log.Printf("Self:       	%s\n", n.address)
	/*for q, value := range n.q {
		log.Printf("Quorum member %d:    %s \n", q, value)
	}*/
	fmt.Println("Database:    ")
	for key, value := range n.database {
		fmt.Printf("***   [%s] ---->  [%s]\n", key, value)
	}
	//log.Println(n.slot)
	for _, value := range n.slot {
		fmt.Sprintf("Position: [%d] \nSequence: [%s] \nOkay: [%s] \nCommand: [%s] \n", value.Position, value.N, value.Decided, value.Data)
	}
	return nil
}
func quit(_ string) error {
	os.Exit(0)
	return nil
}
func (n *Node) create() error {
	rpc.Register(n)
	rpc.HandleHTTP()
	listening := false
	nextAddress := 0
	var l net.Listener
	for !listening {
		nextAddress += 1
		listening = true
		listener, err := net.Listen("tcp", n.address)
		if err != nil {
			if nextAddress >= 5 {
				log.Fatal("Quorum is full")
			}
			listening = false
			n.address = n.q[nextAddress]
			log.Println("Address is:", n.address)
		}
		l = listener
	}
	go http.Serve(l, nil)
	return nil
}
func (n Node) call(address string, method string, request interface{}, reply interface{}) error {

	conn, err := rpc.DialHTTP("tcp", appendLocalHost(address))

	if err != nil {
		return err
	}
	defer conn.Close()
	conn.Call(method, request, reply)

	return nil
}
func randString(n int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz"
	var bytes = make([]byte, n)
	rand.Read(bytes)
	for i, b := range bytes {
		bytes[i] = alphabet[b%byte(len(alphabet))]
	}
	return string(bytes)
}
func (n *Node) putRandom(line string) error {
	//reply := ""
	list := strings.Fields(line)
	num, err := strconv.Atoi(list[0])
	if err != nil {
		// handle error
		return err
	}
	for i := 0; i < num; i++ {
		//switched keys to be numeric to show db consistency
		key := strconv.Itoa(i)
		value := randString(5)
		line = key + " " + value
		//n.putRequest(line)
	}
	return nil
}
func getLocalAddress() string {
	var localaddress string

	ifaces, err := net.Interfaces()
	if err != nil {
		panic("init: failed to find network interfaces")
	}

	// find the first non-loopback interface with an IP address
	for _, elt := range ifaces {
		if elt.Flags&net.FlagLoopback == 0 && elt.Flags&net.FlagUp != 0 {
			addrs, err := elt.Addrs()
			if err != nil {
				panic("init: failed to get addresses for network interface")
			}

			for _, addr := range addrs {
				if ipnet, ok := addr.(*net.IPNet); ok {
					if ip4 := ipnet.IP.To4(); len(ip4) == net.IPv4len {
						localaddress = ip4.String()
						break
					}
				}
			}
		}
	}
	if localaddress == "" {
		panic("init: failed to find non-loopback interface with valid address on this node")
	}

	return localaddress
}
func hashString(elt string) *big.Int {
	hasher := sha1.New()
	hasher.Write([]byte(elt))
	return new(big.Int).SetBytes(hasher.Sum(nil))
}
func appendLocalHost(s string) string {
	if strings.HasPrefix(s, ":") {
		return "127.0.0.1" + s
	} else if strings.Contains(s, ":") {
		return s
	} else {
		return ""
	}
}
func (n Node) testpa(_ string) error {

	/*for i := 0; i < 10; i++ {

																																																																									}*/
	t := n.assemble()
	for i := 0; i < len(t); i++ {
		log.Println(t[i])
	}

	return nil
}
func (n Node) chatLevel(line string) error {
	CHATTY, _ = strconv.Atoi(line)
	return nil
}
func chat(level int, line string) {
	if level >= CHATTY {
		if level == 1 {
			fmt.Println(line)
		} else {
			log.Println(line)
		}
	}
}
func (n Node) assemble() []string {
	quorum := make([]string, 0, len(n.q))

	for _, address := range n.q {
		t := false
		if err := n.call(address, "Node.Ping", "Who cares?", t); err == nil {
			//log.Println(address)
			quorum = Extend(quorum, address)
		}
	}
	return quorum
}
func (n Node) getSlot(slotN int) Slot {
	if value, okay := n.slot[slotN]; okay {
		return value
	} else {
		return Slot{}
	}
}
func (n Node) placeInSlot(elt Slot, num int) bool {
	//num is the index of where to place the slot
	chat(1, fmt.Sprintf("Placing Slot: [%d] ---> [%t]\n", num, elt.Decided))
	n.slot[num] = elt
	return true
}
func Extend(slice []string, element string) []string {
	n := len(slice)
	if n == cap(slice) {
		// Slice is full; must grow.
		// We double its size and add 1, so if the size is zero we still grow.
		newSlice := make([]string, len(slice), 2*len(slice)+1)
		copy(newSlice, slice)
		slice = newSlice
	}
	slice = slice[0 : n+1]
	slice[n] = element
	return slice
}
func (elt Seq) String() string {
	return fmt.Sprintf("Seq number %s from ----> %s", elt.N, elt.Address)
}

//am I greater than the other?
func (elt Seq) Cmp(other Seq) int {
	myNum := elt.N
	otherNum := other.N
	myAddress := elt.Address
	otherAddress := other.Address

	//chat(1, fmt.Sprintf("Comparing %d:%s and %d:%s", myNum, myAddress, otherNum, otherAddress))

	//-1 means less than, 0 means equal, 1 means greater than

	if myNum == otherNum {
		if myAddress > otherAddress {
			return 1
		}
		if myAddress < otherAddress {
			return -1
		}
		return 0
	}
	if myNum > otherNum {
		return 1
	}
	if myNum < otherNum {
		return -1
	}
	return 0
}
func (elt Node) runCommand(com Command) string {

	commandType := com.Type
	switch {
	case commandType == "get":
		if value, exists := elt.database[com.Key]; exists {
			return value
		} else {
			return fmt.Sprintf("Data for: %s     does not exist\n", com.Key)
		}
	case commandType == "put":
		elt.database[com.Key] = com.Value
		return fmt.Sprintf("Put  [%s] -------->  [%s]\n", com.Key, com.Value)
	case commandType == "delete":
		if value, exists := elt.database[com.Key]; exists {
			delete(elt.database, com.Key)
			return fmt.Sprintf("Deleted: [%s] ------> [%s]", com.Key, value)
		} else {
			return fmt.Sprintf("Data for: %s     does not exist\n", com.Key)
		}
	default:
		return fmt.Sprintf("Command type   [%s]   not recognized\n", commandType)
	}
}
func parseInput(line string) (returnC Command, err error) {
	err = nil
	inputs := strings.Fields(line)
	cType := inputs[0]
	id := randString(4)

	returnC.Id = id

	switch {
	case cType == "get" || cType == "delete":
		returnC.Type = cType
		returnC.Key = inputs[1]
	case cType == "put":
		returnC.Type = cType
		returnC.Key = inputs[1]
		returnC.Value = inputs[2]
	default:
		err = errors.New(fmt.Sprintf("Not valid input type --> {%s}", cType))
	}

	return returnC, err
}
func (n Node) put(line string) error {
	line = "put " + line

	nope, err := parseInput(line)
	if err != nil {
		log.Println("cannot put:", err)
		return nil
	}
	n.executePaxos(nope)
	return nil
}
func (elt Node) executePaxos(cmd Command) {

	responseChan := make(chan string, 1)

	// Create the listen for response channel
	elt.Acks[cmd.Id] = responseChan

	pp := Promise{
		Command: cmd,
	}
	message := Request{
		Address: elt.address,
		Promise: pp,
	}
	pReply := PResponse{}

	if err := elt.call(elt.address, "Node.Propose", message, &pReply); err != nil {
		chat(3, "Somehow managed to fail during connecting to myself.\n")
		return
	}
	go func() {
		chat(2, fmt.Sprintf("DONE: [%s]\n", <-elt.Acks[cmd.Id]))
	}()
}
func (elt Command) SameID(other Command) bool {
	// Do the commands conflict?
	return elt.SeqN.Address == other.SeqN.Address && elt.Id == other.Id
}
func (elt Command) SameCommand(other Command) bool {
	return elt.Key == other.Key && elt.Type == other.Type && elt.Value == other.Value
}
func (elt Command) Print() string {
	return fmt.Sprintf("[ %s , %s , %s ]", elt.Type, elt.Key, elt.Value)
}
func allDecided(slots map[int]Slot, n int) bool {
	//n is the slot number to stop at
	if n == 1 {
		return true
	}
	//see if everyone up to this slot is decided
	for i, value := range slots {
		//0 is left empty
		if i == 0 {
			continue
		}
		//we past them all, break and return true
		if i >= n {
			break
		}
		//someone is not decided
		if !value.Decided {
			return false
		}
	}
	return true
}

//1 = yes, 0 = no majority, -1 = no
func (elt Node) majority(yes int, no int) int {
	size := len(elt.q) / 2
	if yes > size {
		return 1
	}
	if no > size {
		return -1
	}
	return 0
}
func (elt Node) Ping(line string, reply *string) error {
	*reply = fmt.Sprintf("[%s] ---> Signing in", elt.address)
	log.Println(*reply)
	return nil
}
func (elt Node) ping(line string) error {
	for _, address := range elt.q {
		reply := ""
		if err := elt.call(address, "Node.Ping", line, &reply); err != nil {
			log.Println("Ping failed:", address)
			continue
		}
		log.Println(reply)
	}
	return nil
}
