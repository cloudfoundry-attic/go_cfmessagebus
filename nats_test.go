package cfmessagebus

import (
	"fmt"
	nats "github.com/cloudfoundry/yagnats"
	. "launchpad.net/gocheck"
	exec "os/exec"
	"strings"
	"time"
)

type AdaptersSuite struct {
	Adapter *NatsAdapter

	natsPort int
	natsCmd  *exec.Cmd
}

var _ = Suite(&AdaptersSuite{})

func NatsRequestResponder(adapter *NatsAdapter, request_subject string, subscribeChan chan bool) {
	serverAdapter := NewNatsAdapter()
	serverAdapter.Configure(adapter.host, adapter.port, adapter.user, adapter.password)
	serverAdapter.Connect()

	serverAdapter.client.Subscribe(request_subject, func(msg *nats.Message) {
		serverAdapter.Publish(msg.ReplyTo, []byte("Response"))
		time.Sleep(500 * time.Millisecond)
		serverAdapter.Publish(msg.ReplyTo, []byte("Second Response"))
	})
	subscribeChan <- true
}

func (s *AdaptersSuite) SetUpTest(c *C) {
	port, err := GrabEphemeralPort()
	if err != nil {
		port = 4223
	}

	s.startNats(port)

	s.Adapter = s.configuredAdapter()
}

func (s *AdaptersSuite) TearDownTest(c *C) {
	if s.natsCmd != nil {
		s.stopNats()
	}
}

func (s *AdaptersSuite) startNats(port int) {
	s.natsPort = port
	s.natsCmd = StartNats(port)

	err := waitUntilNatsUp(port)
	if err != nil {
		panic("cannot connect to NATS")
	}
}

func (s *AdaptersSuite) stopNats() {
	StopNats(s.natsCmd)

	err := waitUntilNatsDown(s.natsPort)
	if err != nil {
		panic("cannot shut down NATS")
	}

	s.natsPort = 0
	s.natsCmd = nil
}

func (s *AdaptersSuite) restartNats() {
	port := s.natsPort
	s.stopNats()
	s.startNats(port)
}

func (s *AdaptersSuite) configuredAdapter() *NatsAdapter {
	adapter := NewNatsAdapter()
	adapter.Configure("127.0.0.1", s.natsPort, "nats", "nats")
	return adapter
}

func (s *AdaptersSuite) TestPingWhenNotConnected(c *C) {
	c.Check(s.Adapter.Ping(), Equals, false)
}

func (s *AdaptersSuite) TestPingWhenNatsIsRunning(c *C) {
	err := s.Adapter.Connect()
	c.Assert(err, IsNil)
	c.Assert(s.Adapter.Ping(), Equals, true)
}

func (s *AdaptersSuite) TestPingWhenNatsIsNotRunning(c *C) {
	s.stopNats()
	c.Check(s.Adapter.Ping(), Equals, false)
}

func (s *AdaptersSuite) TestConnectReturnsNilOnSuccess(c *C) {
	c.Check(s.Adapter.Connect(), IsNil)
}

func (s *AdaptersSuite) TestConnectReturnsErrOnFailure(c *C) {
	s.stopNats()
	c.Check(s.Adapter.Connect(), ErrorMatches, "dial tcp .* connection refused")
}

func (s *AdaptersSuite) TestSubscribe(c *C) {
	receivedChan := make(chan bool, 1)

	messagesReceived := make([]string, 0)

	err := s.Adapter.Connect()
	c.Assert(err, IsNil)

	s.Adapter.Subscribe("some-message", func(payload []byte) {
		messagesReceived = append(messagesReceived, string(payload))
		receivedChan <- true
	})

	s.Adapter.Publish("some-message", []byte("This is a message"))

	failOnTimeout(receivedChan, 1*time.Second, func() {
		c.Check(messagesReceived, DeepEquals, []string{"This is a message"})
	})
}

func (s *AdaptersSuite) TestSubscribeWithNoConnectionCachesSubscriptions(c *C) {
	receivedChan := make(chan bool, 1)

	messagesReceived := make([]string, 0)

	err := s.Adapter.Subscribe("some-message", func(payload []byte) {
		messagesReceived = append(messagesReceived, string(payload))
		receivedChan <- true
	})
	c.Assert(err, NotNil)

	err = s.Adapter.Connect()
	c.Assert(err, IsNil)

	s.Adapter.Publish("some-message", []byte("This is a message"))

	failOnTimeout(receivedChan, 1*time.Second, func() {
		c.Check(messagesReceived, DeepEquals, []string{"This is a message"})
	})
}

func (s *AdaptersSuite) TestPublishWithNoConnection(c *C) {
	adapter := NewNatsAdapter()
	adapter.Configure("127.0.0.1", 4222, "nats", "nats")

	err := adapter.Publish("some-message", []byte("data"))
	c.Assert(err, Not(IsNil))
}

func (s *AdaptersSuite) TestUnsubscribeAllWithNoConnection(c *C) {
	adapter := NewNatsAdapter()
	adapter.Configure("127.0.0.1", 4222, "nats", "nats")

	err := adapter.UnsubscribeAll()
	c.Assert(err, Not(IsNil))
}

func (s *AdaptersSuite) TestRequestWithNoConnection(c *C) {
	adapter := NewNatsAdapter()
	adapter.Configure("127.0.0.1", 4222, "nats", "nats")

	err := adapter.Request("some-message", []byte("data"), func(payload []byte) {
		fmt.Println("CALLBACK!")
	})
	c.Assert(err, Not(IsNil))
}

func (s *AdaptersSuite) TestSettingLogger(c *C) {
	logger := &DefaultLogger{}
	s.Adapter.SetLogger(logger)

	err := s.Adapter.Connect()
	c.Assert(err, IsNil)

	c.Assert(s.Adapter.client.Logger, Equals, logger)
}

func (s *AdaptersSuite) TestConnectedCallback(c *C) {
	connectionChannel := make(chan bool)

	s.Adapter.OnConnect(func() {
		connectionChannel <- true
	})

	err := s.Adapter.Connect()
	c.Assert(err, IsNil)

	failOnTimeout(connectionChannel, 1*time.Second, func() {})
}

func (s *AdaptersSuite) TestReconnectedCallback(c *C) {
	connectionChannel := make(chan bool)

	s.Adapter.OnConnect(func() {
		connectionChannel <- true
	})

	err := s.Adapter.Connect()
	c.Assert(err, IsNil)

	failOnTimeout(connectionChannel, 1*time.Second, func() {})

	s.restartNats()

	failOnTimeout(connectionChannel, 1*time.Second, func() {})
}

func (s *AdaptersSuite) TestPubSubWhenNatsGoesDown(c *C) {
	publisher := s.configuredAdapter()
	err := publisher.Connect()
	c.Assert(err, IsNil)

	subscriber := s.configuredAdapter()
	err = subscriber.Connect()
	c.Assert(err, IsNil)

	messagesReceived := make([]string, 0)
	receivedChan := make(chan bool, 1)

	subscriber.Subscribe("some-message", func(payload []byte) {
		messagesReceived = append(messagesReceived, string(payload))
		receivedChan <- true
	})

	s.restartNats()

	// wait a little bit to give us time to resubscribe
	time.Sleep(1 * time.Second)

	publisher.Publish("some-message", []byte("some message"))

	failOnTimeout(receivedChan, 3*time.Second, func() {
		c.Check(messagesReceived, DeepEquals, []string{"some message"})
	})

	subscriber.UnsubscribeAll()

	publisher.Publish("some-message", []byte("message 2"))

	failOnEvent(receivedChan, 1*time.Second, func() {})

	c.Check(len(messagesReceived), Equals, 1)
}

func (s *AdaptersSuite) TestRequest(c *C) {
	err := s.Adapter.Connect()
	c.Assert(err, IsNil)

	request_subject := "request-subject"
	subscribeBarrier := make(chan bool, 1)

	go NatsRequestResponder(s.Adapter, request_subject, subscribeBarrier)

	messagesReceived := make([]string, 0)
	receivedChan := make(chan bool, 1)
	callback := func(response []byte) {
		messagesReceived = append(messagesReceived, string(response))
		receivedChan <- true
	}

	<-subscribeBarrier
	s.Adapter.Request(request_subject, []byte("request"), callback)

	failOnTimeout(receivedChan, 1*time.Second, func() {
		c.Check(messagesReceived, DeepEquals, []string{"Response"})
	})
}

func (s *AdaptersSuite) TestRespondToChannel(c *C) {
	requestAdapter := s.configuredAdapter()
	err := requestAdapter.Connect()
	c.Assert(err, IsNil)

	respondAdapter := s.configuredAdapter()
	err = respondAdapter.Connect()
	c.Assert(err, IsNil)

	channel := "request-chan"

	respondAdapter.RespondToChannel(channel, func(req []byte) []byte {
		req_string := string(req)
		res_string := strings.ToLower(req_string)
		return []byte(res_string)
	})

	request := "HELLO"
	responseChannel := make(chan bool, 1)
	requestAdapter.Request(channel, []byte(request), func(response []byte) {
		c.Check(string(response), Equals, "hello")
		responseChannel <- true
	})

	failOnTimeout(responseChannel, 2*time.Second, func() {})
}

func (s *AdaptersSuite) TestRespondToChannelWithEmptyReplyChannelIgnoresMessage(c *C) {
	requestAdapter := s.configuredAdapter()
	err := requestAdapter.Connect()
	c.Assert(err, IsNil)

	respondAdapter := s.configuredAdapter()
	err = respondAdapter.Connect()
	c.Assert(err, IsNil)

	channel := "request-chan"
	responseChannel := make(chan bool, 1)

	respondAdapter.RespondToChannel(channel, func(req []byte) []byte {
		responseChannel <- true
		return []byte{}
	})

	requestAdapter.Publish(channel, []byte("HELLO"))

	failOnEvent(responseChannel, 1*time.Second, func() {})
}

func (s *AdaptersSuite) TestRespondToChannelSetupBeforeConnection(c *C) {
	requestAdapter := s.configuredAdapter()
	err := requestAdapter.Connect()
	c.Assert(err, IsNil)

	respondAdapter := s.configuredAdapter()

	channel := "request-chan"

	err = respondAdapter.RespondToChannel(channel, func(req []byte) []byte {
		req_string := string(req)
		res_string := strings.ToLower(req_string)
		return []byte(res_string)
	})
	c.Assert(err, NotNil)

	err = respondAdapter.Connect()
	c.Assert(err, IsNil)

	request := "HELLO"
	responseChannel := make(chan bool, 1)

	requestAdapter.Request(channel, []byte(request), func(response []byte) {
		c.Assert(string(response), Equals, "hello")
		responseChannel <- true
	})

	failOnTimeout(responseChannel, 4*time.Second, func() {})
}
