package fake_cfmessagebus

import (
	"github.com/cloudfoundry/go_cfmessagebus"
)

type FakeMessageBus struct {
	ConnectError          error
	SubscribeError        error
	UnsubscribeAllError   error
	PublishError          error
	RequestError          error
	RespondToChannelError error

	PingResponse bool

	Requests      map[string][]Request
	Subscriptions map[string][]Subscription
}

type Request struct {
	Subject  string
	Message  []byte
	Callback func([]byte)
}

type Subscription struct {
	Subject  string
	Callback func([]byte)
}

func NewFakeMessageBus() *FakeMessageBus {
	bus := &FakeMessageBus{}
	bus.Reset()
	return bus
}

func (bus *FakeMessageBus) Reset() {
	bus.Requests = make(map[string][]Request, 0)
	bus.Subscriptions = make(map[string][]Subscription, 0)
	bus.PingResponse = true
}

func (bus *FakeMessageBus) OnConnect(callback func()) {
	//TODO
	//save off the callback
}

func (bus *FakeMessageBus) Connect() error {
	//TODO
	//call the onConnect callback
	return bus.ConnectError
}

func (bus *FakeMessageBus) Configure(host string, port int, user string, password string) {
	//TODO
}

func (bus *FakeMessageBus) Subscribe(subject string, callback func(payload []byte)) error {
	_, ok := bus.Subscriptions[subject]
	if !ok {
		bus.Subscriptions[subject] = make([]Subscription, 0)
	}
	bus.Subscriptions[subject] = append(bus.Subscriptions[subject], Subscription{
		Subject:  subject,
		Callback: callback,
	})
	return bus.SubscribeError
}

func (bus *FakeMessageBus) UnsubscribeAll() error {
	//TODO
	return bus.UnsubscribeAllError
}

func (bus *FakeMessageBus) Publish(subject string, message []byte) error {
	//TODO
	return bus.PublishError
}

func (bus *FakeMessageBus) Request(subject string, message []byte, callback func(response []byte)) error {
	_, ok := bus.Requests[subject]
	if !ok {
		bus.Requests[subject] = make([]Request, 0)
	}
	bus.Requests[subject] = append(bus.Requests[subject], Request{
		Subject:  subject,
		Message:  message,
		Callback: callback,
	})
	return bus.RequestError
}

func (bus *FakeMessageBus) Ping() bool {
	//TODO
	return bus.PingResponse
}

func (bus *FakeMessageBus) RespondToChannel(subject string, callback func(request []byte) []byte) error {
	//TODO
	return bus.RespondToChannelError
}

func (bus *FakeMessageBus) SetLogger(logger cfmessagebus.Logger) {
	//TODO
}
