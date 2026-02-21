package main

import (
	"advafaeian/go-event-broker/internal/metadata"
	"advafaeian/go-event-broker/internal/server"
	"log"
)

func main() {
	metadata := metadata.NewMetadataLoader("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log")
	srv := server.New("localhost:9092", metadata)
	if err := srv.Start(); err != nil {
		log.Fatal(err)
	}
}
