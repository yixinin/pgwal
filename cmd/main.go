package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"

	"github.com/yixinin/pgwal"
)

func main() {
	var ctx, cancel = context.WithCancel(context.Background())
	var database = "postgres"
	var dsn = fmt.Sprintf("postgres://postgres:1234qwer@postgres:5432/%s?sslmode=disable&replication=database", database)
	slotName := database + "_walslot1"
	pubName := database + "_walcmd"
	repl := pgwal.NewReplica(pubName, slotName, database)
	err := repl.CreateReplica(ctx, dsn)
	if err != nil {
		cancel()
		fmt.Println(err)
		return
	}
	func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Println(r, string(debug.Stack()))
				os.Exit(-1)
			}
		}()
		err = repl.Run(ctx)
		if err != nil {
			fmt.Println(err)
			return
		}
	}()
	var ch = make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	cancel()
	repl.Close(ctx)
}
