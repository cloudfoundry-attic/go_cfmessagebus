package mock_cfmessagebus

import (
	"github.com/cloudfoundry/go_cfmessagebus"
	"github.com/nu7hatch/gouuid"
	"sync"
)

type MockMessageBus struct {
	subscriptions map[string]func([]byte, string)
	onConnect     func()

	lock sync.RWMutex
}

func NewMockMessageBus() *MockMessageBus {
	return &MockMessageBus{
		subscriptions: make(map[string]func([]byte, string)),
	}
}

func (m *MockMessageBus) Configure(host string, port int, user, password string) {
}

func (m *MockMessageBus) Connect() error {
	m.lock.RLock()
	defer m.lock.RUnlock()

	if m.onConnect != nil {
		m.onConnect()
	}

	return nil
}

func (m *MockMessageBus) Subscribe(subject string, callback func([]byte)) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.subscriptions[subject] = func(payload []byte, reply string) {
		callback(payload)
	}

	return nil
}

func (m *MockMessageBus) UnsubscribeAll() error {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.subscriptions = make(map[string]func([]byte, string))
	return nil
}

func (m *MockMessageBus) Publish(subject string, message []byte) error {
	m.lock.RLock()
	defer m.lock.RUnlock()

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
	return true
}

func (m *MockMessageBus) RespondToChannel(subject string, callback func([]byte) []byte) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.subscriptions[subject] = func(payload []byte, reply string) {
		m.Publish(reply, callback(payload))
	}

	return nil
}

func (m *MockMessageBus) publishWithReply(subject string, message []byte, reply string) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	callback, present := m.subscriptions[subject]
	if !present {
		return
	}

	go callback(message, reply)

	return
}

func (m *MockMessageBus) OnConnect(callback func()) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.onConnect = callback
}

func (m *MockMessageBus) SetLogger(logger cfmessagebus.Logger) {
}
