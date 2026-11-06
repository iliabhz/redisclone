package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type item struct {
	val string
	exp time.Time
}

var kv map[string]item

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	kv = make(map[string]item)
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
		kv[key] = item{
			val: val,
			exp: time.Now().Add(time.Millisecond * time.Duration(*exp)),
		}
	} else {
		kv[key] = item{val: val}
	}

	conn.Write([]byte("+OK\r\n"))
}

func handleGetCMD(conn net.Conn, key string) {
	item, ok := kv[key]
	if !ok {
		conn.Write([]byte("$-1\r\n"))
		return
	}

	if !item.exp.IsZero() && time.Now().After(item.exp) {
		delete(kv, key)
		conn.Write([]byte("$-1\r\n"))
		return
	}

	conn.Write([]byte(BuildBulkString(item.val).String()))
}
