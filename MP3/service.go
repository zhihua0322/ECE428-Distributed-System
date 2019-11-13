//this is the file which handles different kinds of request
//including DeleteService GetService and PutService
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)

type DeleteService struct{}
type GetService struct{}
type PutService struct{}

//this service function will return a timestamp which can be used to check time bound
func (p *PutService) CheckPutTime(request string, reply *string) error {
	fmt.Println(fileList[request].TimeStamp)
	if _, ok := fileList[request]; ok {
		*reply = strconv.FormatInt(fileList[request].TimeStamp, 10)
	} else {
		*reply = strconv.FormatInt(time.Now().Unix()-70, 10)
	}
	return nil
}

func (p *DeleteService) Delete(request string, reply *string) error {
	err := os.Remove(request)
	if (err != nil) {
		fmt.Println("delete error!", err)
	}
	return nil
}

//forward delete to nodes have the file
func (p *DeleteService) ForwardDelete(request string, reply *string) error {
	_, index := Getip()
	memid := election()
	if _, ok := fileList[request]; ok {
		nodelist := fileList[request].NodeList
		for _, i := range nodelist {
			if i == index {
				os.Remove(request)
			} else {
				client, err := rpc.Dial("tcp", straddr[i]+":4000")
				if (err != nil) {
					fmt.Println(err)
				}
				err = client.Call("DeleteService.Delete", request, &reply)
				if err != nil {
					fmt.Println(err)
				}
				*reply = "file delete!"
				defer client.Close()
			}
		}
		for _, index1 := range memid {
			if index1 == index {
				delete(fileList, request)
			} else {
				var temp = make(map[string]pair)
				temp[request] = pair{
					TimeStamp: 0,
					NodeList:  nil,
				}
				sendlist(temp, straddr[index1])
			}
		}
	} else {
		*reply = "no such file!"
	}
	return nil
}

//send file to the request node
func (p *GetService) Get(request string, reply *string) error {
	var sdfsfilename string
	var localfilename string
	var IP string
	IP = strings.Split(request, " ")[0]
	sdfsfilename = strings.Split(request, " ")[1]
	localfilename = strings.Split(request, " ")[2]
	time.Sleep(time.Second * 1)
	putfile(sdfsfilename, localfilename, IP)
	return nil
}

//send filelist to the request node
func (p *GetService) GetFileList(request string, reply *string) error {
	buffer := make([]byte, 1024)
	buffer, _ = json.Marshal(fileList)
	*reply = string(buffer)
	return nil
}

//always-on receivefile function
func revFile(fileName string, conn net.Conn) {
	defer conn.Close()
	fs, err := os.Create(fileName)
	defer fs.Close()
	if err != nil {
		fmt.Println("os.Create err =", err)
		return
	}
	buf := make([]byte, 1024*10)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				fmt.Println("the end of file", err)
			}
			return
		}
		if n == 0 {
			return
		}
		fs.Write(buf[:n])
	}
}
