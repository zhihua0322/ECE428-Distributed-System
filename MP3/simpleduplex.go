package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

//the definition of the membership table
//it include timestamp,counter and a status value
type membership struct {
	Counter int64
	Time    int64
	Status  bool
}

//table: is a table maintained by self
var table sync.Map
var myid string
//different types for log file
var (
	Join        *log.Logger
	Failurerecv *log.Logger
	Failuredet  *log.Logger
	Leave       *log.Logger
)
//this is a array which is used for defining the name of log file and find the introducer
// no other use
var straddr []string = []string{
	"172.22.152.166", "172.22.154.162", "172.22.156.162",
	"172.22.152.167", "172.22.154.163", "172.22.156.163",
	"172.22.152.168", "172.22.154.164", "172.22.156.164",
	"172.22.152.169"}

//function to get self ip
func GetIp() (string, int) {
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
						index = i+1
					}
				}
			}
		}
	}
	return ip, index
}

//change an entry to invalid
func ChangeStatus(ID string) {
	value, _ := table.Load(ID)
	newvalue := value.(membership)
	table.Store(ID, membership{
		Counter: newvalue.Counter,
		Time:    newvalue.Time,
		Status:  false,
	})
}

//change syncmap to bytes
func SyncMapToByte(input sync.Map) []byte {
	buffer := make([]byte, 1024)
	sendtable := make(map[string]membership)
	input.Range(func(key, value interface{}) bool {
		newkey := fmt.Sprintf("%v", key)
		newvalue := value.(membership)
		sendtable[newkey] = newvalue
		return true
	})
	buffer, _ = json.Marshal(sendtable)
	return buffer
}
func SizeofTable(p sync.Map) int {
	var count int
	table.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}

//function which will time out an entry and clean an entry
func DeleteEntry(interval float64) {
	for {
		var nodes []string
		table.Range(func(key, value interface{}) bool {
			newkey := fmt.Sprintf("%v", key)
			newvalue := value.(membership)
			if (float64(time.Now().Unix()-newvalue.Time) > interval && key != myid) || newvalue.Status == false {
				nodes = append(nodes, newkey)
				if newvalue.Status == true {
					Failuredet.Println(newkey)
				}
				ChangeStatus(newkey)
			}
			return true
		})
		//the time between time out and clean
		time.Sleep(2e9)
		for _, node := range nodes {
			table.Delete(node)
			ip := getIpFromID(node)
			reReplicate(indexOfstring(ip, straddr))
		}
	}
}
//function which will merge a table received
func ReceiveHeartbeat() {
	udp_addr, _ := net.ResolveUDPAddr("udp", ":8888")
	conn, _ := net.ListenUDP("udp", udp_addr)
	defer conn.Close()
	data := make([]byte, 1024)
	for {
		n, _, err := conn.ReadFromUDP(data)
		if err != nil {
			fmt.Println(err)
		}
		msg := make(map[string]membership)
		json.Unmarshal(data[0:n], &msg)
		merge(msg)
	}
}

//the function which can send heartbeat to a host
func SendHeartbeat(addr string) {
	conn, err := net.Dial("udp", addr+":8888")
	if err != nil {
		fmt.Println(err)
	}
	defer conn.Close()
	myRecord, _ := table.Load(myid)
	count := myRecord.(membership).Counter
	count++
	table.Store(myid, membership{
		Counter: count,
		Time:    time.Now().Unix(),
		Status:  true,
	})
	rand.Seed(time.Now().Unix())
	randomnumber := rand.Intn(100)
	if(randomnumber>=0){
		conn.Write([]byte(SyncMapToByte(table)))
	}
}
func InitTable(done chan bool) {
	myip, _ := GetIp()
	timestamp := time.Now().Unix()
	myid = myip + " " + strconv.FormatInt(timestamp, 10)
	table.Store(myid, membership{
		Counter: 0,
		Time:    timestamp,
		Status:  true,
	})
	selfintro()
	done <- true
}
func merge(msg map[string]membership) {
	for k, v := range msg {
		vv, ok := table.Load(k)
		if ok {
			newvv := vv.(membership)
			if newvv.Status == true {
				if v.Status == true && newvv.Counter < v.Counter {
					v.Time = time.Now().Unix()
					table.Store(k, v)
				} else if v.Status == false {
					ChangeStatus(k)
					Failurerecv.Println(k)
				}
			}
		} else {
			if v.Status == true {
				v.Time = time.Now().Unix()
				table.Store(k, v)
				Join.Println(k)
			}
		}
	}
}
//initialize the log function
func init() {
	_, machineindex := GetIp()
	var logname string = "VM" + strconv.Itoa(machineindex) + ".log"
	vmFile, err := os.OpenFile(logname, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("Fail to open log file：", err)
	}
	Join = log.New(io.MultiWriter(os.Stderr, vmFile), "Join:", log.Ldate|log.Ltime)
	Leave = log.New(io.MultiWriter(os.Stderr, vmFile), "Leave:", log.Ldate|log.Ltime)
	Failuredet = log.New(io.MultiWriter(os.Stderr, vmFile), "Failuredetected:", log.Ldate|log.Ltime)
	Failurerecv = log.New(io.MultiWriter(os.Stderr, vmFile), "Failurereceived:", log.Ldate|log.Ltime)

}
func getmembership() (int, []string) {
	var count int = 0
	var memaddr []string
	table.Range(func(k, v interface{}) bool {
		if k.(string) != myid {
			count++
			addr := strings.Split(k.(string), " ")
			memaddr = append(memaddr, addr[0])
		}
		return true
	})
	return count, memaddr
}
func contains(s []string, e string) bool {
	for _, a := range s {
		if strings.EqualFold(a, e) {
			return true
		}
	}
	return false
}
func generatesendaddr() []string {
	count, memaddr := getmembership()
	if count <= 3 {
		return memaddr
	}
	var sendaddrlist []string
	rand.Seed(time.Now().Unix())
	a := 0
	for {
		random := rand.Intn(count - 1)
		if !contains(sendaddrlist, memaddr[random]) {
			sendaddrlist = append(sendaddrlist, memaddr[random])
			a++
		}
		if a == 3 {
			break
		}
	}
	return sendaddrlist
}

//at beginning will do a slef introduction
func selfintro() {
	myip, _ := GetIp()
	if straddr[0] != myip {
		srcAddr := &net.UDPAddr{IP: net.IPv4zero, Port: 1234}
		ip := net.ParseIP(straddr[0])
		dstAddr := &net.UDPAddr{IP: ip, Port: 8888}
		conn, err := net.DialUDP("udp", srcAddr, dstAddr)
		if err != nil {
			fmt.Println(err)
		}
		defer conn.Close()
		conn.Write(SyncMapToByte(table))
	}
}
func checkError(err error) {
	if err != nil {
		fmt.Println("Error: %s", err.Error())
		os.Exit(1)
	}
}

//wait for an input from terminal and terminates the application
//then send a message
//func waitforcommand() {
//	buffer := make([]byte, 2048)
//	for {
//		n, err := os.Stdin.Read(buffer[:])
//		if err != nil {
//			fmt.Println("read error:", err)
//			return
//		}
//		if string(buffer[0:n]) == "leave\n" {
//			//send leave message to others
//			srcAddr := &net.UDPAddr{IP: net.IPv4zero, Port: 1233}
//			count, memaddr := getmembership()
//			for i := 0; i < count; i++ {
//				ip := net.ParseIP(memaddr[i])
//				dstAddr := &net.UDPAddr{IP: ip, Port: 4444}
//				conn, err := net.DialUDP("udp", srcAddr, dstAddr)
//				checkError(err)
//				conn.Write([]byte(myid))
//				conn.Close()
//			}
//			Leave.Println(myid)
//			os.Exit(0)
//		} else if string(buffer[0:4]) == "grep" {
//			//used for grep command
//			str := buffer[5 : n-1]
//			grepcall(string(str))
//		}else if string(buffer[0:n]) == "exit\n"{
//			currentime := time.Now()
//			fmt.Println("Exit:",currentime.Format("2006/01/02 15：04：05"))
//			os.Exit(1)
//		}else if string(buffer[0:n]) == "print\n"{
//			table.Range(func(k, v interface{}) bool {
//				fmt.Println(k,v)
//				return true
//			})
//		}
//	}
//}

//wait for a leave message and broadcast the message
func rcvleave() {
	udp_addr, _ := net.ResolveUDPAddr("udp", ":4444")
	conn, _ := net.ListenUDP("udp", udp_addr)
	defer conn.Close()
	data := make([]byte, 1024)
	for {
		n, _, err := conn.ReadFromUDP(data)
		if err != nil {
			fmt.Println(err)
		}
		Leave.Println(string(data[:n]))
		table.Delete(string(data[:n]))
	}
}
func updateMembership() {
	var wg sync.WaitGroup
	done := make(chan bool, 1)
	go InitTable(done)
	<-done
	go ReceiveHeartbeat()
	go DeleteEntry(3)
	//go waitforcommand()
	go rcvleave()
	go grepservice()
	var dur time.Duration = 0.5 * 1e9
	chRate := time.Tick(dur)
	for {
		<-chRate
		sendaddr := generatesendaddr()
		for _, addr := range sendaddr {
			go SendHeartbeat(addr)
		}
	}
	wg.Add(4)
	wg.Wait()
}
