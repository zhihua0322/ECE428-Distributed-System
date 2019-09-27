package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

//the IP address of every VM
var straddr []string = []string{
	"172.22.152.166", "172.22.154.162", "172.22.156.162",
	"172.22.152.167", "172.22.154.163", "172.22.156.163",
	"172.22.152.168", "172.22.154.164", "172.22.156.164",
	"172.22.152.169"}

func Getip() (string, int) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	var ip string
	var index int
	for _, address := range addrs {

		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				for i := 0; i < len(straddr); i++ {
					if ipnet.IP.String() == straddr[i] {
						ip = straddr[i]
						index = i
					}
				}
			}
		}
	}
	return ip, index
}

func Comm(s string, str string, wg *sync.WaitGroup, i int, a *[10] int) {
	client, err := rpc.Dial("tcp", s)
	if err != nil {
		fmt.Println("Dialing:", err)
		wg.Done()
		return
	}
	defer client.Close()
	var reply string
	c := make(chan error, 1)
	//timeout mechanism
	go func() {
		c <- client.Call("GrepService.Grep", str, &reply)
	}()
	select {
	//print and put the result into a[i] if succeed
	case err := <-c:
		if err != nil {
			log.Fatal(err)
		}
		//wipe off \n in the string for turning string into integer
		val, err := strconv.Atoi(strings.Replace(reply, "\n", "", -1))
		a[i] = val
		fmt.Println("vm", i, ":", "the counts of", str, "is", reply)
	//print timeout and put 0 into a[i] if no response in 5 seconds
	case <-time.After(time.Second * 5):
		fmt.Println("Timeout")
		a[i] = 0
	}
	wg.Done()
}

func Commtest(s string, str string, wg *sync.WaitGroup, i int, a *[10] int) {
	client, err := rpc.Dial("tcp", s)
	if err != nil {
		fmt.Println("Dialing:", err)
		wg.Done()
		return
	}
	defer client.Close()
	//client := rpc.NewClient(conn);
	var reply string
	c := make(chan error, 1)
	go func() {
		c <- client.Call("TestService.Test", str, &reply)
	}()
	select {
	case err := <-c:
		if err != nil {
			log.Fatal(err)
		}
		val, err := strconv.Atoi(strings.Replace(reply, "\n", "", -1))
		a[i] = val
		fmt.Println("vm", i, ":", "the counts of", str, "is", reply)
	case <-time.After(time.Second * 5):
		// call timed out
		fmt.Println("timeout")
		a[i] = 0
		//return "timeout"
	}
	wg.Done()
}

func main() {
	t1 := time.Now() // get current time
	//get the pattern or regulation expression
	str := os.Args[1]
	var wg sync.WaitGroup
	sum := 0
	var a [10] int
	_, machinindex := Getip()
	//communicate with every server and fetch the result in a [9] int
	for i, addr := range straddr {
		wg.Add(1)
		if i == machinindex {
			go Comm("127.0.0.1:3333", str, &wg, i, &a)
		} else {
			go Comm(addr+":3333", str, &wg, i, &a)
		}
	}

	wg.Wait()
	//calculate the sum
	for _, v := range a {
		sum += v
	}
	fmt.Println("The sum is", sum)
	elapsed := time.Since(t1)
	fmt.Println("App elapsed: ", elapsed)
}
