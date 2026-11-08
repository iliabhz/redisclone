package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type item struct {
	val string
	exp time.Time
}

type listItem struct {
	val  string
	list string
}

var itemkv map[string]item

var listkv map[string][]string
var itemInsertedChan chan listItem
var listkvMu sync.Mutex

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	itemkv = make(map[string]item)
	itemInsertedChan = make(chan listItem, 1)
	listkv = make(map[string][]string)
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConn(conn)
	}

}

type BulkString struct {
	len int
	str string
}

func NewBulkString(len int, str string) BulkString {
	return BulkString{
		len: len,
		str: str,
	}
}

func BuildBulkString(str string) BulkString {
	return BulkString{
		len: len(str),
		str: str,
	}
}

func (s BulkString) String() string {
	if s.str == "" {
		return "$-1\r\n"
	}
	return fmt.Sprintf("$%d\r\n%s\r\n", s.len, s.str)
}

func handleConn(conn net.Conn) {
	defer conn.Close()

	buf := make([]byte, 1024)

	for {
		n, err := conn.Read(buf)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				fmt.Println("Error reading from connection: ", err.Error())
			}
			break
		}

		if n == 0 {
			continue
		}

		tempBuf := make([]byte, n)
		copy(tempBuf, buf[:n])
		rd := bytes.NewBuffer(tempBuf)

		rd.ReadByte() // read the *
		arrCountStr, err := rd.ReadString('\r')
		rd.ReadByte() // consume the \n after \r
		arrCountStr = strings.Trim(arrCountStr, "\r")

		if err != nil {
			fmt.Printf("Failed to read arrCountStr: %s\n", err.Error())
		}
		arrCount, err := strconv.Atoi(arrCountStr)
		if err != nil {
			fmt.Printf("Failed to convert arrCountStr to int: %s\n", err.Error())
		}

		resps := make([]BulkString, 0, arrCount)
		for i := 0; i < arrCount; i++ {
			lenStr, _ := rd.ReadString('\r')
			rd.ReadByte() // consume the \n after \r
			lenStr = strings.TrimPrefix(lenStr, "$")
			lenStr = strings.Trim(lenStr, "\r")
			len, err := strconv.Atoi(lenStr)
			if err != nil {
				fmt.Printf("Failed to read lenStr in RESP: %s\n", err.Error())
			}
			str, err := rd.ReadString('\r')
			if err != nil {
				fmt.Printf("Failed to read RESP str: %s\n", err.Error())
			}
			rd.ReadByte() // consume the \n after \r
			str = strings.Trim(str, "\r")
			bs := NewBulkString(len, str)
			resps = append(resps, bs)
		}
		processCommands(conn, resps)
	}
}

func processCommands(conn net.Conn, cmds []BulkString) {
	switch strings.ToUpper(cmds[0].str) {
	case "PING":
		handlePingCMD(conn)
	case "ECHO":
		handleEchoCMD(conn, cmds[1].str) // cmds[1] is the arguement
	case "SET":
		if len(cmds) == 5 {
			if strings.ToUpper(cmds[3].str) == "EX" {
				ms, err := strconv.Atoi(cmds[4].str)
				if err != nil {
					fmt.Printf("Failed parsing EX value for SET cmd: %s", err.Error())
					break
				}
				ms *= 1000

				handleSetCMD(conn, cmds[1].str, cmds[2].str, &ms)
				break
			}
			if strings.ToUpper(cmds[3].str) == "PX" {
				ms, err := strconv.Atoi(cmds[4].str)
				if err != nil {
					fmt.Printf("Failed parsing PX value for SET cmd: %s", err.Error())
					break
				}

				handleSetCMD(conn, cmds[1].str, cmds[2].str, &ms)
				break
			}
		}
		handleSetCMD(conn, cmds[1].str, cmds[2].str, nil)
	case "GET":
		handleGetCMD(conn, cmds[1].str)
	case "RPUSH":
		vals := make([]string, 0, len(cmds)-2)
		for i := 2; i < len(cmds); i++ {
			vals = append(vals, cmds[i].str)
		}
		handleRPushCMD(conn, cmds[1].str, vals)
	case "LRANGE":
		startIdx, err := strconv.Atoi(cmds[2].str)
		if err != nil {
			fmt.Printf("Failed parsing start index for LRANGE cmd: %s\n", err.Error())
			break
		}
		stopIdx, err := strconv.Atoi(cmds[3].str)
		if err != nil {
			fmt.Printf("Failed parsing stop index for LRANGE cmd: %s\n", err.Error())
			break
		}

		handleLRangeCMD(conn, cmds[1].str, startIdx, stopIdx)
	case "LPUSH":
		vals := make([]string, 0, len(cmds)-2)
		for i := 2; i < len(cmds); i++ {
			vals = append(vals, cmds[i].str)
		}

		handleLPushCMD(conn, cmds[1].str, vals)
	case "LLEN":
		handleLLenCMD(conn, cmds[1].str)
	case "LPOP":
		if len(cmds) == 3 {
			count, err := strconv.Atoi(cmds[2].str)
			if err != nil {
				fmt.Printf("Failed parsing count for LPOP cmd: %s\n", err.Error())
				break
			}
			handleLPopCMD(conn, cmds[1].str, count)
			return
		}
		handleLPopCMD(conn, cmds[1].str)
	case "BLPOP":
		timeout, err := strconv.Atoi(cmds[2].str)
		if err != nil {
			fmt.Printf("Failed parsing timeout for BLPOP cmd: %s\n", err.Error())
			break
		}
		handleBLPopCMD(conn, cmds[1].str, timeout)
	}
}

func handlePingCMD(conn net.Conn) {
	conn.Write([]byte("+PONG\r\n"))
}

func handleEchoCMD(conn net.Conn, msg string) {
	conn.Write([]byte(BuildBulkString(msg).String()))
}

func handleSetCMD(conn net.Conn, key string, val string, exp *int) {
	if exp != nil {
		itemkv[key] = item{
			val: val,
			exp: time.Now().Add(time.Millisecond * time.Duration(*exp)),
		}
	} else {
		itemkv[key] = item{val: val}
	}

	conn.Write([]byte("+OK\r\n"))
}

func handleGetCMD(conn net.Conn, key string) {
	item, ok := itemkv[key]
	if !ok {
		conn.Write([]byte("$-1\r\n"))
		return
	}

	if !item.exp.IsZero() && time.Now().After(item.exp) {
		delete(itemkv, key)
		conn.Write([]byte("$-1\r\n"))
		return
	}

	conn.Write([]byte(BuildBulkString(item.val).String()))
}

func handleRPushCMD(conn net.Conn, key string, vals []string) {
	listkvMu.Lock()
	list, ok := listkv[key]
	listkvMu.Unlock()
	if !ok {
		list = make([]string, 0, 1)
	}

	for i := range vals {
		list = append(list, vals[i])
	}

	listkvMu.Lock()
	listkv[key] = list
	itemInsertedChan <- listItem{val: list[0], list: key}
	listkvMu.Unlock()

	writeInteger(conn, len(list))
}

func writeInteger(conn net.Conn, num int) {
	fmt.Fprintf(conn, ":%d\r\n", num)
}

func handleLRangeCMD(conn net.Conn, key string, startIdx int, stopIdx int) {
	listkvMu.Lock()
	list, ok := listkv[key]
	listkvMu.Unlock()
	if !ok {
		writeArray(conn, nil)
		return
	}

	if startIdx < 0 {
		startIdx = len(list) + startIdx
	}
	if stopIdx < 0 {
		stopIdx = len(list) + stopIdx
	}

	if startIdx < 0 {
		startIdx = 0
	}
	if stopIdx < 0 {
		stopIdx = 0
	}

	if startIdx >= len(list) || startIdx > stopIdx {
		writeArray(conn, nil)
		return
	}
	if stopIdx >= len(list) {
		stopIdx = len(list) - 1
	}

	writeArray(conn, list[startIdx:stopIdx+1])
}

func writeArray(conn net.Conn, arr []string) {
	var resp strings.Builder
	resp.WriteString(fmt.Sprintf("*%d\r\n", len(arr)))

	for i := range arr {
		resp.WriteString(BuildBulkString(arr[i]).String())
	}

	conn.Write([]byte(resp.String()))
}

func handleLPushCMD(conn net.Conn, key string, vals []string) {
	listkvMu.Lock()
	list, ok := listkv[key]
	listkvMu.Unlock()
	if !ok {
		list = make([]string, 0, 1)
	}

	for i := range vals {
		temp := []string{vals[i]}
		temp = appendList(temp, list)
		list = temp
	}

	listkvMu.Lock()
	listkv[key] = list
	itemInsertedChan <- listItem{val: list[0], list: key}
	listkvMu.Unlock()

	writeInteger(conn, len(list))
}

func appendList(a, b []string) []string {
	for i := range b {
		a = append(a, b[i])
	}
	return a
}

func handleLLenCMD(conn net.Conn, key string) {
	listkvMu.Lock()
	list, ok := listkv[key]
	listkvMu.Unlock()
	if !ok {
		writeInteger(conn, 0)
		return
	}

	writeInteger(conn, len(list))
}

func handleLPopCMD(conn net.Conn, key string, count ...int) {
	listkvMu.Lock()
	list, ok := listkv[key]
	listkvMu.Unlock()
	if !ok {
		conn.Write([]byte(BuildBulkString("").String()))
		return
	}
	if count == nil {
		a := list[0]

		listkvMu.Lock()
		listkv[key] = list[1:]
		listkvMu.Unlock()

		conn.Write([]byte(BuildBulkString(a).String()))
		return
	}

	if count[0] > len(list) {
		count[0] = len(list)
	}

	elems := make([]string, 0, count[0])
	for i := 0; i < count[0]; i++ {
		elems = append(elems, list[i])
	}

	listkvMu.Lock()
	listkv[key] = list[count[0]:]
	listkvMu.Unlock()

	writeArray(conn, elems)
}

func handleBLPopCMD(conn net.Conn, key string, timeout int) {
	ctx := context.Background()
	if timeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), time.Second*time.Duration(timeout))
		defer cancel()
	}

	listkvMu.Lock()
	lval, ok := listkv[key]
	listkvMu.Unlock()
	if ok {
		v := lval[0]
		listkvMu.Lock()
		listkv[key] = lval[1:]
		listkvMu.Unlock()
		itemInsertedChan <- listItem{val: v, list: key}
	}

	select {
	case <-itemInsertedChan:
		it := <-itemInsertedChan
		writeArray(conn, []string{it.list, it.val})
		return
	case <-ctx.Done():
		conn.Write([]byte("*-1\r\n"))
		return
	}
}
