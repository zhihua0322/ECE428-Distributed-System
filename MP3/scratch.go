package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

//fileList is to store sdfsfile and the information of the file
var fileList = make(map[string]pair)

//the pair struct comtains timestamp which is the store time of the file and
//nodelist which is the list of places the file is stored
type pair struct {
	TimeStamp int64
	NodeList  []int
}

//hash function is used to convert string to integer
func hash(s string) int {
	counts, _ := getIndexList()
	var hash int
	hash = 11
	var c int
	for _, v := range s {
		c = int(v)
		hash = ((hash << 5) + hash) + c /* hash * 33 + c */
	}
	return hash % counts
}

//generate storelist of the file
func generateStoreList(s string) []int {
	//first get the indexlist from the membership list
	_, indexList := getIndexList()
	_, maxIndex := MinMax(indexList)
	var list []int
	//generate first node
	h := hash(s)
	list = append(list, h)
	//append two more nodes to the storelist
	for i := 0; i < 2; {
		//make sure that the index of node is within the alive nodes
		h = (h + 1) % (maxIndex + 1)
		if containsint(indexList, h) {
			list = append(list, h)
			i++
		}
	}
	return list
}

//check whether one integer is in one integer slice
func containsint(s []int, e int) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

//main function for ls command
func ls(filename string) []int {
	_, indexList := getIndexList()
	_, maxIndex := MinMax(indexList)
	//find masters from membership
	memid := election()
	var value pair
	var fileMap = make(map[string]pair)
	//find the index of this VM
	_, i := Getip()
	//check whether this VM is the master
	if !containsint(memid, i) {
		//this VM is not a master
		//call master to request fileList
		client, err := rpc.Dial("tcp", straddr[maxIndex]+":4000")
		if (err != nil) {
			fmt.Println("ls dial getfilelist error", err)
		}
		defer client.Close()
		var reply string
		client.Call("GetService.GetFileList", straddr[i], &reply)
		//eliminate "\n" from reply for correct Unmarshal
		reply = strings.Replace(reply, "\n", "", -1)
		buf := []byte(reply)
		json.Unmarshal(buf, &fileMap)
		if _, ok := fileMap[filename]; ok {
			//file exits in SDFS
			value = fileMap[filename]
		} else {
			//file not exits in SDFS
			//return pair contains nil nodelist
			value = pair{
				TimeStamp: 0,
				NodeList:  nil,
			}
		}
	} else {
		//this VM is a master
		//so we can pick nodelist from local
		if _, ok := fileList[filename]; ok {
			//file exits in SDFS
			value = fileList[filename]
		} else {
			//file not exits in SDFS
			//return pair contains nil nodelist
			value = pair{
				TimeStamp: 0,
				NodeList:  nil,
			}
		}
	}
	return value.NodeList
}

//main function for put command
func put(sdfsfilename string, localfilename string) {
	_, indexList := getIndexList()
	memid := election()
	_, maxIndex := MinMax(indexList)
	_, i := Getip()
	var putTime string
	var n int64
	var temp = make(map[string]pair)
	if !containsint(memid, i) {
		client, err := rpc.Dial("tcp", straddr[maxIndex]+":4000")
		if (err != nil) {
			fmt.Println("putfile checkputtime dial delete error", err)
		}
		defer client.Close()
		//call master to request last put time of this file
		client.Call("PutService.CheckPutTime", sdfsfilename, &putTime)
		//convert string to int64
		n, _ = strconv.ParseInt(putTime, 10, 64)
	} else {
		if _, ok := fileList[sdfsfilename]; ok {
			n = fileList[sdfsfilename].TimeStamp
		} else {
			//return a time long before now
			//so the judge function in prompt function won't be triggered
			n = time.Now().Unix() - 70
		}
	}
	//check whether this put time is within 1 minute from last put time and wait for the decision of user
	decision := prompt(sdfsfilename, n)
	//if the user decide not to put
	if decision == false {
		return
	}
	//if the user decide to put or it is out of 1 minute
	storeList := generateStoreList(sdfsfilename)
	for _, v := range storeList {
		//if the file is at local dir
		//just copy the file
		if v == i {
			copyFile(sdfsfilename, localfilename)
		} else {
			//or transmit the file to another node
			putfile(localfilename, sdfsfilename, straddr[v])
		}
	}
	if decision == true {
		//generate the timestamp and nodelist of this file
		temp[sdfsfilename] = pair{
			TimeStamp: time.Now().Unix(),
			NodeList:  storeList,
		}
	}
	//send the filelist to masters
	for _, v := range memid {
		if v == i {
			//update filelist locally if this VM is master
			fileList[sdfsfilename] = temp[sdfsfilename]
		} else {
			//send list to master
			sendlist(temp, straddr[v])
		}
	}
}

//put file to another node
func putfile(localfilename, dfsname, IP string) {
	conn, err1 := net.Dial("tcp", IP+":4001")
	if err1 != nil {
		fmt.Println("net.Dial err = ", err1)
		return
	}
	defer conn.Close()
	//firstly, transmit sdfsname to that node
	conn.Write([]byte (dfsname))
	buf := make([]byte, 1024)
	n, err2 := conn.Read(buf)
	if err2 != nil {
		return
	}
	//after that node responses with ok
	if "ok" == string(buf[:n]) {
		//send the whole file to that node
		sendFile(localfilename, conn)
	}
}
func sendFile(path string, conn net.Conn) {
	defer conn.Close()
	//open local file
	fs, err := os.Open(path)
	defer fs.Close()
	if err != nil {
		fmt.Println("os.Open err = ", err)
		return
	}
	buf := make([]byte, 1024*10)
	//transmit the whole file
	for {
		n, err1 := fs.Read(buf)
		if err1 != nil {
			return
		}
		conn.Write(buf[:n])
	}
}

//send filelist
func sendlist(list map[string]pair, IP string) {
	buf := make([]byte, 1024)
	buf, err := json.Marshal(list)
	conn, err := net.Dial("tcp", IP+":4002")
	if err != nil {
		fmt.Println("dial error", err)
	}
	conn.Write(buf)
}

//main function for delete command
func deletefile(sdfsfilename string) {
	_, indexList := getIndexList()
	_, maxIndex := MinMax(indexList)
	memid := election()
	_, i := Getip()
	var reply string
	if !containsint(memid, i) {
		//if this VM is not master
		//rpc call master's forward delete
		client, err := rpc.Dial("tcp", straddr[maxIndex]+":4000")
		if (err != nil) {
			fmt.Println("deletefile dial forwarddelete error", err)
		}
		defer client.Close()
		client.Call("DeleteService.ForwardDelete", sdfsfilename, &reply)
	} else {
		if _, ok := fileList[sdfsfilename]; ok {
			nodelist := fileList[sdfsfilename].NodeList
			for _, node := range nodelist {
				if node == i {
					os.Remove(sdfsfilename)
				} else {
					client, err := rpc.Dial("tcp", straddr[node]+":4000")
					if (err != nil) {
						fmt.Println("deletefile dial delete error", err)
					}
					defer client.Close()
					client.Call("DeleteService.Delete", sdfsfilename, &reply)
				}
			}
			for _, index := range memid {
				if index == i {
					delete(fileList, sdfsfilename)
				} else {
					var temp = make(map[string]pair)
					temp[sdfsfilename] = pair{
						TimeStamp: 0,
						NodeList:  nil,
					}
					sendlist(temp, straddr[index])
				}
			}
		} else {
			fmt.Println("No such file")
		}
	}
}

//main function for get command
func get(sdfsfilename string, localfilename string) {
	//get nodelist of the file
	nodelist := ls(sdfsfilename)
	if nodelist == nil {
		fmt.Println("No such file")
		return
	}
	var reply string
	ip, index := Getip()
	if containsint(nodelist, index) {
		copyFile(localfilename, sdfsfilename)
	} else {
		client, err := rpc.Dial("tcp", straddr[nodelist[0]]+":4000")
		if (err != nil) {
			fmt.Println("get dial error", err)
		}
		defer client.Close()
		client.Call("GetService.Get", ip+" "+sdfsfilename+" "+localfilename, &reply)
	}
}

//copyfile locally
func copyFile(dstName, srcName string) (written int64, err error) {
	src, err := os.Open(srcName)
	if err != nil {
		return
	}
	defer src.Close()
	dst, err := os.OpenFile(dstName, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return
	}
	defer dst.Close()
	return io.Copy(dst, src)
}

//main function for store command
func store(id int) []string {
	_, indexList := getIndexList()
	_, maxIndex := MinMax(indexList)
	var reply string
	var fileMap = make(map[string]pair)
	var fileStored []string
	memid := election()
	_, i := Getip()
	if !containsint(memid, i) {
		client, err := rpc.Dial("tcp", straddr[maxIndex]+":4000")
		if (err != nil) {
			fmt.Println("store dial error", err)
		}
		defer client.Close()
		client.Call("GetService.GetFileList", straddr[i], &reply)
		reply = strings.Replace(reply, "\n", "", -1)
		buf := []byte(reply)
		json.Unmarshal(buf, &fileMap)
		//traverse the filelist to find files stored at this node
		for k, v := range fileMap {
			if containsint(v.NodeList, id) {
				fileStored = append(fileStored, k)
			}
		}
	} else {
		for k, v := range fileList {
			if containsint(v.NodeList, id) {
				fileStored = append(fileStored, k)
			}
		}
	}
	return fileStored
}

//prompt function
func prompt(filename string, putTime int64) bool {
	//use select to achieve timeout
	if time.Now().Unix()-putTime < 60 {
		c1 := make(chan bool, 1)
		go func() {
			buffer := make([]byte, 2048)
			fmt.Print("Last put is within 1 min, are you sure putting this file now?[y/n]")
			n, err := os.Stdin.Read(buffer[:])
			if err != nil {
				fmt.Println("read error:", err)
			}
			if string(buffer[0:n]) == "y\n" {
				c1 <- true
			} else if string(buffer[0:n]) == "n\n" {
				c1 <- false
			} else {
				c1 <- false
			}
		}()

		select {
		//return the decision of user
		case res := <-c1:
			return res
		//if timeout
		//don't put the file
		case <-time.After(30 * time.Second):
			fmt.Println("timeout")
			return false
		}

	}
	return true
}

func getIpFromID(id string) string {
	addr := strings.Split(id, " ")
	return addr[0]
}

func getIplistFromIDlist(idList []string) []string {
	var ipList []string
	for _, v := range idList {
		ipList = append(ipList, getIpFromID(v))
	}
	return ipList
}

func getIndexFromIplist(ipList []string) []int {
	var indexList []int
	for _, v := range ipList {
		indexList = append(indexList, indexOfstring(v, straddr))
	}
	return indexList
}

//find min and max of certain slice
func MinMax(array []int) (int, int) {
	var max int = array[0]
	var min int = array[0]
	for _, value := range array {
		if max < value {
			max = value
		}
		if min > value {
			min = value
		}
	}
	return min, max
}

//find index of certain string in one string slice
func indexOfstring(element string, data []string) int {
	for k, v := range data {
		if element == v {
			return k
		}
	}
	return -1 //not found.
}

//find index of certain integer in one integer slice
func indexOfint(element int, data []int) int {
	for k, v := range data {
		if element == v {
			return k
		}
	}
	return -1 //not found.
}

//get indexlist from membership
func getIndexList() (int, []int) {
	_, memaddr := getmembership()
	var memid = getIndexFromIplist(memaddr)
	i, _ := Getip()
	memid = append(memid, indexOfstring(i, straddr))
	sort.Ints(memid)
	return len(memid), memid
}

//find masters
func election() []int {
	counts, memid := getIndexList()
	if counts < 4 {
		return memid
	} else {
		//masters are the VMs with the most three index
		return memid[len(memid)-3:]
	}
}

//when some node is down
//we should rereplicate file stored at that node
func reReplicate(id int) {
	_, i := Getip()
	_, indexList := getIndexList()
	_, maxIndex := MinMax(indexList)
	memaddr := election()
	//only master need to run rereplicate function
	if i == maxIndex {
		fileStored := store(id)
		var reply string
		//traverse stored file
		for _, v := range fileStored {
			nodeList := fileList[v].NodeList
			_, maxNode := MinMax(nodeList)
			var nextNode int
			//nextnode is maxnode plus 1 but it should within maxindex
			nextNode = (maxNode + 1) % (maxIndex + 1)
			for i := 0; ; i++ {
				//loop until satisfy this condition
				if containsint(indexList, nextNode) && !containsint(nodeList, nextNode) {
					index := indexOfint(id, nodeList)
					//update filelist
					fileList[v].NodeList[index] = nextNode
					//boundary check
					if index == 0 {
						index = index + 1
					} else {
						index = index - 1
					}
					client, err := rpc.Dial("tcp", straddr[nodeList[index]]+":4000")
					if (err != nil) {
						fmt.Println("rereplicate dial error", err)
					}
					defer client.Close()
					//call that node store the file to transmit file to nextnode
					client.Call("GetService.Get", straddr[nextNode]+" "+v+" "+v, &reply)
					break
				}
				nextNode = (nextNode + 1) % (maxIndex + 1)
			}
		}
		//send updated filelist to other masters
		sendlist(fileList, straddr[memaddr[0]])
		sendlist(fileList, straddr[memaddr[1]])
	}
}

func waitForDFSCommand() {
	buffer := make([]byte, 2048)
	var arg []string
	var localfilename string
	var sdfsfilename string
	for {
		n, err := os.Stdin.Read(buffer[:])
		if err != nil {
			fmt.Println("read error:", err)
			return
		}
		if string(buffer[0:3]) == "put" {
			arg = strings.Split(string(buffer[0:n]), " ")
			localfilename = arg[1]
			sdfsfilename = arg[2]
			sdfsfilename = strings.Replace(sdfsfilename, "\n", "", -1)
			fmt.Println(localfilename, sdfsfilename)
			put(sdfsfilename, localfilename)
		} else if string(buffer[0:3]) == "get" {
			arg = strings.Split(string(buffer[0:n]), " ")
			sdfsfilename = arg[1]
			localfilename = arg[2]
			localfilename = strings.Replace(localfilename, "\n", "", -1)
			fmt.Println(sdfsfilename, localfilename)
			get(sdfsfilename, localfilename)
		} else if string(buffer[0:6]) == "delete" {
			arg = strings.Split(string(buffer[0:n]), " ")
			sdfsfilename = arg[1]
			sdfsfilename = strings.Replace(sdfsfilename, "\n", "", -1)
			fmt.Println(sdfsfilename)
			deletefile(sdfsfilename)
		} else if string(buffer[0:2]) == "ls" {
			arg = strings.Split(string(buffer[0:n]), " ")
			sdfsfilename = arg[1]
			sdfsfilename = strings.Replace(sdfsfilename, "\n", "", -1)
			nodelist := ls(sdfsfilename)
			if nodelist != nil {
				fmt.Printf("%s is stored at", sdfsfilename)
				fmt.Println(nodelist)
			} else {
				fmt.Println("No such file")
			}
		} else if string(buffer[0:n]) == "store\n" {
			_, i := Getip()
			filelist := store(i)
			fmt.Printf("%d stores", i)
			fmt.Println(filelist)
		} else if string(buffer[0:n]) == "leave\n" {
			//send leave message to others
			srcAddr := &net.UDPAddr{IP: net.IPv4zero, Port: 1233}
			count, memaddr := getmembership()
			for i := 0; i < count; i++ {
				ip := net.ParseIP(memaddr[i])
				dstAddr := &net.UDPAddr{IP: ip, Port: 4444}
				conn, err := net.DialUDP("udp", srcAddr, dstAddr)
				checkError(err)
				conn.Write([]byte(myid))
				conn.Close()
			}
			Leave.Println(myid)
			os.Exit(0)
		} else if string(buffer[0:4]) == "grep" {
			//used for grep command
			str := buffer[5 : n-1]
			grepcall(string(str))
		} else if string(buffer[0:n]) == "exit\n" {
			currentime := time.Now()
			fmt.Println("Exit:", currentime.Format("2006/01/02 15：04：05"))
			os.Exit(1)
		} else if string(buffer[0:n]) == "print\n" {
			table.Range(func(k, v interface{}) bool {
				fmt.Println(k, v)
				return true
			})
		}
	}
}
func waitforfile() {
	Server, err := net.Listen("tcp", ":4001")

	if err != nil {
		fmt.Println("net.Listen err =", err)
		return
	}
	defer Server.Close()
	for {
		//accept tcp connection
		conn, err := Server.Accept()
		defer conn.Close()
		if err != nil {
			fmt.Println("Server.Accept err =", err)
			return
		}
		buf := make([]byte, 1024)
		n, err1 := conn.Read(buf)
		if err1 != nil {
			fmt.Println("conn.Read err =", err1)
			return
		}
		fileName := string(buf[:n])
		//receive filename
		conn.Write([]byte ("ok"))
		//start receive file
		revFile(fileName, conn)
	}
}

//update, increase and delete list entry
func updatelist() {
	Server, err := net.Listen("tcp", ":4002")
	if err != nil {
		fmt.Println("net.Listen err =", err)
		return
	}
	defer Server.Close()
	for {
		//accept tcp connection
		conn, err := Server.Accept()
		defer conn.Close()
		if err != nil {
			fmt.Println("Server.Accept err =", err)
			return
		}
		buf := make([]byte, 1024)
		n, err1 := conn.Read(buf)
		if err1 != nil {
			fmt.Println("conn.Read err =", err1)
			return
		}
		//create temp list for storing received list
		list := make(map[string]pair)
		json.Unmarshal(buf[:n], &list)
		//traverse received list to update local list
		for key, value := range list {
			//if timestamp is 0, which means we should delete this entry
			if value.TimeStamp == 0 {
				delete(fileList, key)
			} else {
				//update this entry or create this entry
				fileList[key] = list[key]
			}
		}
	}
}
func removeAllfile(dir string) error {
	//open the directory
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	//obtain all dir names
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	//traverse all names and remove files that don't satisfy these conditions
	for _, name := range names {
		if strings.Contains(name, "go") || strings.Contains(name, "file") || strings.Contains(name, "md") ||
			strings.Contains(name, "git") || strings.Contains(name, "idea") || strings.Contains(name, "tgz") {
			continue
		}
		err = os.RemoveAll(filepath.Join(dir, name))
		if err != nil {
			return err
		}
	}
	return nil
}

//wait for rpc calls
func rpcservice(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Accept error:", err)
		}
		rpc.ServeConn(conn)
		conn.Close()
	}
}
func main() {
	//register rpc services
	rpc.RegisterName("GetService", new(GetService))
	rpc.RegisterName("DeleteService", new(DeleteService))
	rpc.RegisterName("PutService", new(PutService))
	listener, err := net.Listen("tcp", ":4000")
	if err != nil {
		fmt.Println("Listen error:", err)
	}
	err = removeAllfile("../mp3")
	if err != nil {
		fmt.Println("Remove all file error:", err)
	}
	var wg sync.WaitGroup
	go waitForDFSCommand()
	go updateMembership()
	go waitforfile()
	go updatelist()
	go rpcservice(listener)
	wg.Add(5)
	wg.Wait()

}
