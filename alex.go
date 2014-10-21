package main

import (
	"bufio"
	//"crypto/rand"
	//"flag"
	//"fmt"
	"log"
	//"math/big"
	//"net"
	//"net/http"
	//"net/rpc"
	"os"
	//"strconv"
	"strings"
	//"time"
)

const SIZE = 100

type Node struct {
	address  string
	q        []string //quorum
	ledger   map[int]string
	database map[string]string
}

func (n Node) help(line string) error {
	log.Print("      The list of commands are:      ")
	log.Print("  put <key>      , <value>, <address>")
	log.Print("  putrandom <n>  , get <key>         ")
	log.Print("  delete <key>   , dump              ")
	log.Print("	 get <key> 		, <value> <address> ")
	return nil
}
func (n Node) dump(_ string) error {
	/*for _, value := range n.q {
		log.Println(value)
	}*/
	log.Println("Quorum is", n.q)
	return nil
}
func quit(_ string) error {
	os.Exit(0)
	return nil
}
func main() {
	addrin := os.Args

	var node = &Node{
		address:  "",
		q:        make([]string, 5),
		ledger:   make(map[int]string),
		database: make(map[string]string),
	}
	for i := 0; i < 5; i++ {
		node.q[i] = appendLocalHost(addrin[i+1])
	}
	node.address = node.q[0]
	log.Println(node.address)
	node.create()
	m := map[string]func(string) error{
		"help": node.help,
		//"put":       node.putRequest,
		//"putrandom": node.putRandom,
		//"get":    node.getRequest,
		//"delete": node.deleteRequest,
		"dump": node.dump,
		"quit": quit,
	}

	for {
		reader := bufio.NewReader(os.Stdin)
		line, err := reader.ReadString('\n')
		line = strings.TrimSpace(line)
		if err != nil {
			log.Fatal("Can't read string!", err)
		}
		if line == "" {
			node.help("")
		} else {
			str := strings.Fields(line)
			line = strings.Join(str[1:], " ")
			if _, ok := m[str[0]]; ok {
				m[str[0]](line)
			}
		}
	}
}
