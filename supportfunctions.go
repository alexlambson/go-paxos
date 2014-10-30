package main

import (
	//"bufio"
	"crypto/rand"
	//"flag"
	"fmt"
	"log"
	"math/big"
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
	for q, value := range n.q {
		log.Printf("Quorum member %d:    %s \n", q, value)
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
		key := randString(5)
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
