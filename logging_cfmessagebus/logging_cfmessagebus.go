package logging_cfmessagebus

import (
	"fmt"
	"github.com/cloudfoundry/go_cfmessagebus"
)

type LoggingMessageBus struct{}

func (bus *LoggingMessageBus) OnConnect(callback func()) {
}

func (bus *LoggingMessageBus) Connect() error {
	return nil
}

func (bus *LoggingMessageBus) Configure(host string, port int, user string, password string) {
}

func (bus *LoggingMessageBus) Subscribe(subject string, callback func(payload []byte)) error {
	panic("Can't subscribe with the logging-only message bus")
}

func (bus *LoggingMessageBus) UnsubscribeAll() error {
	return nil
}

func (bus *LoggingMessageBus) Publish(subject string, message []byte) error {
	fmt.Printf("SENDING MESSAGE: %s, %s\n", subject, string(message))
	return nil
}

func (bus *LoggingMessageBus) Request(subject string, message []byte, callback func(response []byte)) error {
	panic("Can't request with the logging-only message bus")
}

func (bus *LoggingMessageBus) Ping() bool {
	return true
}

func (bus *LoggingMessageBus) RespondToChannel(subject string, callback func(request []byte) []byte) error {
	return nil
}

func (bus *LoggingMessageBus) SetLogger(logger cfmessagebus.Logger) {
}
