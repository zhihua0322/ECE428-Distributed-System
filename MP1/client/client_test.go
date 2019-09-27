package main

import (
	"reflect"
	"sync"
	"testing"
)

func TestComm(t *testing.T) {
	//the test result
	var a [10] int
	var b [10] int
	var c [10] int
	var d [10] int
	//the true result
	var resa = [10]int{}
	var resb = [10]int{}
	var resc = [10]int{}
	var resd = [10]int{}
	for m := 0; m < 10; m++ {
		resa[m] = 111
		resb[m] = 222
		resc[m] = 333
		resd[m] = 444
	}
	var wg sync.WaitGroup
	_, machinindex := Getip()
	//run test function in client
	for i, addr := range straddr{
		if i == machinindex {
			wg.Add(1)
			go Commtest("127.0.0.1:3333", "'^[0-9]+[a-z]'", &wg, i, &a)
			wg.Add(1)
			go Commtest("127.0.0.1:3333", "Mozilla", &wg, i, &b)
			wg.Add(1)
			go Commtest("127.0.0.1:3333", "www", &wg, i, &c)
			wg.Add(1)
			go Commtest("127.0.0.1:3333", "'(a)(b)(ac).* (a)(b)(ac)'", &wg, i, &d)
		} else {
			wg.Add(1)
			go Commtest(addr+":3333", "'^[0-9]+[a-z]'", &wg, i, &a)
			wg.Add(1)
			go Commtest(addr+":3333", "Mozilla", &wg, i, &b)
			wg.Add(1)
			go Commtest(addr+":3333", "www", &wg, i, &c)
			wg.Add(1)
			go Commtest(addr+":3333", "'(a)(b)(ac).* (a)(b)(ac)'", &wg, i, &d)
		}
	}

	wg.Wait()
	//compare the test result and true result
	if reflect.DeepEqual(a, resa) && reflect.DeepEqual(b, resb) && reflect.DeepEqual(c, resc) && reflect.DeepEqual(d, resd){
		t.Log("The result is ok")
	} else {
		t.Fatal("The result is wrong")
	}
}

