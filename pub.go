package pgwal

import (
	"context"
	"fmt"
)

type Publisher interface {
	Send(ctx context.Context, data []byte) error
	SendAsync(data []byte, f func(data []byte, err error))
}

type PrintPub struct {
}

func (PrintPub) Send(ctx context.Context, data []byte) error {
	fmt.Println(string(data))
	return nil
}

func (PrintPub) SendAsync(data []byte, f func(data []byte, err error)) {
	fmt.Println(string(data))
}
