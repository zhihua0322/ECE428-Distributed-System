package main

import (
	"fmt"
	"log"
	"net/rpc"
	"strconv"
	"strings"
	"sync"
	"time"
)

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
		fmt.Println("vm", i+1, ":", str, "is", reply)
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

func grepcall(str string) {
	t1 := time.Now() // get current time
	//get the pattern or regulation expression
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
