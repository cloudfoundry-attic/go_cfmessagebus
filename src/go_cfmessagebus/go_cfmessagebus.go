package go_cfmessagebus

import (
	"fmt"
)

type CFMessageBus interface {
	Configure(host string, port int, user string, password string)
	Connect() error
	Subscribe(subject string, callback func(payload []byte)) error
	UnsubscribeAll() error
	Publish(subject string, message []byte) error
	Request(subject string, message []byte, callback func(response []byte)) error
	Ping() bool
	RespondToChannel(subject string, callback func(request []byte) []byte) error
}

// MyError is an error implementation that includes a time and message.
type WrongAdapterError struct {
	badAdapter string
}

func (e WrongAdapterError) Error() string {
	return fmt.Sprintf("Adapter: %s not found", e.badAdapter)
}

func NewCFMessageBus(adapter string) (CFMessageBus, error) {
	if adapter == "NATS" {
		return NewNatsAdapter(), nil
	}
	return nil, WrongAdapterError{badAdapter: adapter}
}
