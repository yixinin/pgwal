package pgwal

import (
	"context"
	"fmt"
)

type Publisher interface {
	Send(ctx context.Context, data []byte) error
	SendAsync(data []byte)
}

type MockPub struct {
}

func (MockPub) Send(ctx context.Context, data []byte) error {
	fmt.Println(string(data))
	return nil
}

func (MockPub) SendAsync(data []byte) {
	fmt.Println(string(data))
}
