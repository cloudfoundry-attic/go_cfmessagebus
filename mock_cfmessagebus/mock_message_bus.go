package mock_cfmessagebus

import (
	"github.com/cloudfoundry/go_cfmessagebus"
	"github.com/nu7hatch/gouuid"
	"sync"
)

type MockMessageBus struct {
	subscriptions map[string]func([]byte, string)
	onConnect     func()
	onPing        func() bool

	sync.RWMutex
}

func NewMockMessageBus() *MockMessageBus {
	return &MockMessageBus{
		subscriptions: make(map[string]func([]byte, string)),
	}
}

func (m *MockMessageBus) Configure(host string, port int, user, password string) {
}

func (m *MockMessageBus) Connect() error {
	m.RLock()
	defer m.RUnlock()

	if m.onConnect != nil {
		m.onConnect()
	}

	return nil
}

func (m *MockMessageBus) Subscribe(subject string, callback func([]byte)) error {
	m.Lock()
	defer m.Unlock()

	m.subscriptions[subject] = func(payload []byte, reply string) {
		callback(payload)
	}

	return nil
}

func (m *MockMessageBus) UnsubscribeAll() error {
	m.Lock()
	defer m.Unlock()

	m.subscriptions = make(map[string]func([]byte, string))
	return nil
}

func (m *MockMessageBus) Publish(subject string, message []byte) error {
	m.RLock()
	defer m.RUnlock()

	m.publishWithReply(subject, message, "")

	return nil
}

func (m *MockMessageBus) Request(subject string, message []byte, callback func([]byte)) error {
	reply, err := uuid.NewV4()
	if err != nil {
		return err
	}

	err = m.Subscribe(reply.String(), callback)
	if err != nil {
		return err
	}

	m.publishWithReply(subject, message, reply.String())

	return nil
}

func (m *MockMessageBus) Ping() bool {
	if m.onPing != nil {
		return m.onPing()
	}

	return true
}

func (m *MockMessageBus) RespondToChannel(subject string, callback func([]byte) []byte) error {
	m.Lock()
	defer m.Unlock()

	m.subscriptions[subject] = func(payload []byte, reply string) {
		m.Publish(reply, callback(payload))
	}

	return nil
}

func (m *MockMessageBus) publishWithReply(subject string, message []byte, reply string) {
	m.RLock()
	defer m.RUnlock()

	callback, present := m.subscriptions[subject]
	if !present {
		return
	}

	go callback(message, reply)

	return
}

func (m *MockMessageBus) OnConnect(callback func()) {
	m.Lock()
	defer m.Unlock()

	m.onConnect = callback
}

func (m *MockMessageBus) OnPing(callback func() bool) {
	m.Lock()
	defer m.Unlock()

	m.onPing = callback
}

func (m *MockMessageBus) SetLogger(logger cfmessagebus.Logger) {
}
