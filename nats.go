package cfmessagebus

import (
	"errors"
	"fmt"
	nats "github.com/cloudfoundry/yagnats"
	"math/rand"
	"time"
)

type NatsAdapter struct {
	client        *nats.Client
	host          string
	user          string
	port          int
	password      string
	subscriptions []*Subscription
	rand          *rand.Rand

	connectedCallback func()
	logger            Logger
}

type Subscription struct {
	subject  string
	callback func([]byte)
	reply    func([]byte) []byte
	id       int
}

func NewNatsAdapter() *NatsAdapter {
	return &NatsAdapter{}
}

func (adapter *NatsAdapter) Configure(host string, port int, user string, password string) {
	adapter.host = host
	adapter.port = port
	adapter.user = user
	adapter.password = password
}

func (adapter *NatsAdapter) Connect() error {
	return adapter.connect()
}

func (adapter *NatsAdapter) OnConnect(callback func()) {
	adapter.connectedCallback = callback
}

func (adapter *NatsAdapter) SetLogger(logger Logger) {
	adapter.logger = logger
}

func (adapter *NatsAdapter) connect() error {
	addr := fmt.Sprintf("%s:%d", adapter.host, adapter.port)

	client := nats.NewClient()

	client.ConnectedCallback = func() {
		if adapter.connectedCallback != nil {
			adapter.connectedCallback()
		}
	}

	if adapter.logger != nil {
		client.Logger = adapter.logger
	}

	err := client.Connect(&nats.ConnectionInfo{
		Addr:     addr,
		Username: adapter.user,
		Password: adapter.password,
	})

	if err != nil {
		return err
	}

	adapter.client = client
	adapter.rand = rand.New(rand.NewSource(time.Now().UnixNano()))

	for _, sub := range adapter.subscriptions {
		subscribeInNats(adapter, sub)
	}

	return nil
}

func (adapter *NatsAdapter) createInbox() string {
	return fmt.Sprintf("_INBOX.%04x%04x%04x%04x%04x%06x",
		adapter.rand.Int31n(0x10000), adapter.rand.Int31n(0x10000), adapter.rand.Int31n(0x10000),
		adapter.rand.Int31n(0x10000), adapter.rand.Int31n(0x10000), adapter.rand.Int31n(0x1000000))
}

func (adapter *NatsAdapter) Subscribe(subject string, callback func(payload []byte)) error {
	sub := &Subscription{subject: subject, callback: callback}
	adapter.subscriptions = append(adapter.subscriptions, sub)

	if adapter.client != nil {
		subscribeInNats(adapter, sub)
	} else {
		return errors.New("No connection to Nats. Caching subscription...")
	}

	return nil
}

func (adapter *NatsAdapter) UnsubscribeAll() error {
	return withConnectionCheck(adapter.client, func() {
		for _, sub := range adapter.subscriptions {
			adapter.client.UnsubscribeAll(sub.subject)
		}
	})
}

func (adapter *NatsAdapter) Publish(subject string, message []byte) error {
	return withConnectionCheck(adapter.client, func() {
		adapter.client.Publish(subject, string(message))
	})
}

func (adapter *NatsAdapter) Request(subject string, message []byte, callback func(payload []byte)) error {
	return withConnectionCheck(adapter.client, func() {
		inbox := adapter.createInbox()
		adapter.Subscribe(inbox, callback)
		adapter.client.PublishWithReplyTo(subject, string(message), inbox)
	})
}

func (adapter *NatsAdapter) RespondToChannel(subject string, replyCallback func([]byte) []byte) error {
	sub := &Subscription{subject: subject, reply: replyCallback}
	adapter.subscriptions = append(adapter.subscriptions, sub)

	if adapter.client != nil {
		subscribeInNats(adapter, sub)
	} else {
		return errors.New("No connection to Nats. Caching subscription...")
	}

	return nil
}

func (adapter *NatsAdapter) Ping() bool {
	return adapter.client.Ping()
}

func withConnectionCheck(connection *nats.Client, callback func()) error {
	if connection == nil {
		return errors.New("No connection to Nats")
	}

	callback()
	return nil
}

func subscribeInNats(adapter *NatsAdapter, sub *Subscription) {
	sid, _ := adapter.client.Subscribe(sub.subject, func(msg *nats.Message) {
		if sub.reply != nil {
			adapter.client.Publish(msg.ReplyTo, string(sub.reply([]byte(msg.Payload))))
		} else {
			sub.callback([]byte(msg.Payload))
		}
	})

	sub.id = sid
}
