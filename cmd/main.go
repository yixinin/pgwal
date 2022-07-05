package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yixinin/pgwal"
)

func main() {
	var ctx, cancel = context.WithCancel(context.Background())
	var opts = &pgwal.Options{
		Database: "postgres",
		AppName:  "cmd",

		Host:        "postgres",
		Port:        5432,
		User:        "postgres",
		ReadTimeout: 5 * time.Second,
		Tables: []string{
			"ttb",
			"ttb1",
		},
	}
	repl := pgwal.NewReplica(pgwal.MockPub{}, opts)

	var ch = make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			err := repl.Run(ctx)
			if err != nil {
				log.Println(err)
			}
			if err == nil || errors.Is(err, context.Canceled) {
				return
			}
			// after 10 seconds, restart
			log.Println("restarting ...")
			time.Sleep(10 * time.Second)
		}
	}()
	<-ch
	cancel()

	err := repl.Close(ctx)
	if err != nil {
		log.Println(err)
	}
}
