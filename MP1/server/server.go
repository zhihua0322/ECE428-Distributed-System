package main

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"strconv"
)
//the IP address of every VM
var straddr []string = []string{
	"172.22.152.166", "172.22.154.162", "172.22.156.162",
	"172.22.152.167", "172.22.154.163", "172.22.156.163",
	"172.22.152.168", "172.22.154.164", "172.22.156.164",
	"172.22.152.169"}

type GrepService struct{}
type TestService struct{}

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
					if (ipnet.IP.String() == straddr[i]) {
						ip = straddr[i]
						index = i
					}
				}
			}
		}
	}
	return ip, index
}

func (serve *TestService) Test(request string, reply *string) error {
	//This is running the python program which can generate some specific and random strings, using for test
	cmd := exec.Command("python", "Reggen.py")
	cmd.Run()
	if request[0:1] == "'" {
		newstr := request[1 : len(request)-1]
		cmd = exec.Command("grep", "-c", "-E", newstr, "test.log")
	} else {
		cmd = exec.Command("grep", "-c", request, "test.log")
	}
	//cmd := exec.Command("grep", "-c", request, "vm1.log")
	fmt.Println("new request for: " + request)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		fmt.Println("Execute Command failed:" + err.Error())
		*reply = "0"
		return nil
	}
	fmt.Println(out.String())
	fmt.Println("Execute Command finished.")
	*reply = out.String()
	fmt.Println(*reply)
	return nil
}

func (p *GrepService) Grep(request string, reply *string) error {
	_, machineindex := Getip()
	machineindex++
	i:= strconv.Itoa(machineindex)
	filename := "vm" + i+".log"
	var cmd *exec.Cmd
	//distinguish the type of tests: pattern or regular expression
	if request[0:1] == "'" {
		newstr := request[1 : len(request)-1]
		cmd = exec.Command("grep", "-c", "-E", newstr, filename)
	} else {
		cmd = exec.Command("grep", "-c", request, filename)
	}
	fmt.Println("new request for: " + request)
	//execute the request command
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	//fail if not find the pattern
	if err != nil {
		fmt.Println("Execute Command failed:" + err.Error())
		*reply = "0"
		return nil
	}
	fmt.Println("Execute Command finished.")
	*reply = out.String()
	fmt.Println("The result is "+*reply)
	return nil
}


func main() {
	//register for the two kinds of service which can be used by rpc
	rpc.RegisterName("GrepService", new(GrepService))
	rpc.RegisterName("TestService", new(TestService))
	//use tcp connection for data transfer
	listener, err := net.Listen("tcp", ":3333")
	if err != nil {
		log.Fatal("Listen error:", err)
	}
	//concurrent server
	for{
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal("Accept error:", err)
		}
		rpc.ServeConn(conn)
	}
}
