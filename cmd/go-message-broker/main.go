package main

import (
	"fmt"
	"log"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func intToBytes(n int32) []byte {
	return []byte{
		byte(n >> 24),
		byte(n >> 16),
		byte(n >> 8),
		byte(n),
	}
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		log.Fatal("Failed to bind to port 9092")
	}
	defer l.Close()

	var c net.Conn
	for {
		c, err = l.Accept()
		go func() {
			defer c.Close()
			if err != nil {
				log.Fatal("Error accepting connection: ", err.Error())
			}
			var correlation_id = make([]byte, 12)
			c.Read(correlation_id)

			correlation_id = correlation_id[8:12]
			response := append(intToBytes(0), correlation_id...)

			c.Write(response)
		}()

	}

}
