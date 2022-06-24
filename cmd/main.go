package main

import (
	"context"
	"fmt"
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
	}
	repl := pgwal.NewReplica(pgwal.PrintPub{}, opts)

	var ch = make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := repl.Run(ctx); err != nil {
			fmt.Println(err)
		}
		ch <- os.Interrupt
	}()
	<-ch
	cancel()

	err := repl.Close(ctx)
	if err != nil {
		fmt.Println(err)
	}
}
