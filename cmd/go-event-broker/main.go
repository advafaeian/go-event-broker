package main

import (
	"advafaeian/go-event-broker/internal/server"
	"log"
)

func main() {

	srv := server.New(":9092")
	if err := srv.Start(); err != nil {
		log.Fatal(err)
	}
}
