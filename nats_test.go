package go_cfmessagebus

import (
  "fmt"
  "errors"
  //"strconv"
  exec "os/exec"
  "net"
  "time"
  . "launchpad.net/gocheck"
  nats "github.com/apcera/nats"
  "strings"
)

type AdaptersSuite struct{}
var _ = Suite(&AdaptersSuite{})

func waitUntilNatsDown(port int) error {
  maxWait := 10
  for i := 0; i < maxWait; i++ {
    time.Sleep(500 * time.Millisecond)
    _, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
    if err != nil {
      return nil
    }
  }
  return errors.New("Waited too long for NATS to stop")
}

func failOnTimeout(successChan chan bool, timeout time.Duration, onSuccess func()) {
  onTimeout := func() { panic("Timed out") }
  withTimeout(successChan, timeout, onSuccess, onTimeout)
}

func failOnEvent(eventChan chan bool, timeout time.Duration, onTimeout func()) {
  onEvent := func() { panic("Event should not have happened") }
  withTimeout(eventChan, timeout, onEvent, onTimeout)
}

func withTimeout(successChan chan bool, timeout time.Duration, onSuccess func(), onTimeout func()) {
  timeoutChan := make(chan bool, 1)
  go func() {
    time.Sleep(timeout)
    timeoutChan <- true
  }()

  select {
  case <-successChan:
    onSuccess()
  case <-timeoutChan:
    onTimeout()
  } 
}

func NatsRequestResponder(adapter *NatsAdapter, request_subject string, subscribeChan chan bool) {
  serverAdapter := NewNatsAdapter()
  serverAdapter.Configure(adapter.host, adapter.port, adapter.user, adapter.password)
  serverAdapter.Connect()

  serverAdapter.client.Subscribe(request_subject, func(msg *nats.Msg) {
    serverAdapter.Publish(msg.Reply, []byte("Response"))
    time.Sleep(500 * time.Millisecond)
    serverAdapter.Publish(msg.Reply, []byte("Second Response"))
  })
  subscribeChan <- true
}

func before() (*NatsAdapter, *exec.Cmd) {
  port := 4223
  cmd := StartNats(port)

  adapter := NewNatsAdapter()
  adapter.Configure("127.0.0.1", port, "nats", "nats")
  adapter.Connect()

  return adapter, cmd
}

func after(cmd *exec.Cmd) {
  StopNats(cmd)
}

func (s *AdaptersSuite) TestPingWhenNatsIsRunning(c *C) {
  adapter, cmd := before()
  defer after(cmd)

  reachable := adapter.Ping()
  c.Check(reachable, Equals, true)
}

func (s *AdaptersSuite) TestPingWhenNatsIsNotRunning(c *C) {
  adapter, cmd := before()
  after(cmd)

  reachable := adapter.Ping()
  c.Check(reachable, Equals, false)
}

func (s *AdaptersSuite) TestConnectReturnsNilOnSuccess(c *C) {
  port := 4223
  cmd := StartNats(port)
  defer StopNats(cmd)

  adapter := NewNatsAdapter()
  adapter.Configure("127.0.0.1", port, "nats", "nats")

  c.Check(adapter.Connect(), IsNil)
}

func (s *AdaptersSuite) TestConnectReturnsErrOnFailure(c *C) {
  adapter := NewNatsAdapter()
  adapter.Configure("127.0.0.1", 4223, "nats", "nats")

  c.Check(adapter.Connect(), ErrorMatches, "nats: No servers available for connection")  
}

func (s *AdaptersSuite) TestSubscribe(c *C) {
  adapter, nats_cmd := before()
  defer after(nats_cmd)

  receivedChan := make(chan bool, 1)

  messagesReceived := make([]string, 0)
  adapter.Subscribe("some-message", func (payload []byte) {
    messagesReceived = append(messagesReceived, string(payload))
    receivedChan <- true
  })

  adapter.Publish("some-message", []byte("This is a message"))

  failOnTimeout(receivedChan, 1 * time.Second, func() {
    c.Check(len(messagesReceived), Equals, 1)
    c.Check(messagesReceived[0], Equals, "This is a message")
  })
}

func (s *AdaptersSuite) TestSubscribeWithNoConnection(c *C) {
  adapter := NewNatsAdapter()
  adapter.Configure("127.0.0.1", 4222, "nats", "nats")

  receivedChan := make(chan bool, 1)
  messagesReceived := make([]string, 0)
  err := adapter.Subscribe("some-message", func(payload []byte) {
    messagesReceived = append(messagesReceived, string(payload))
    receivedChan <- true
  })
  c.Assert(err, Not(IsNil))

  cmd := StartNats(4222)
  defer after(cmd)
  adapter.Connect()

  adapter.Publish("some-message", []byte("This is a message"))

  failOnTimeout(receivedChan, 1 * time.Second, func() {
    c.Check(len(messagesReceived), Equals, 1)
    c.Check(messagesReceived[0], Equals, "This is a message")
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

func (s *AdaptersSuite) TestPubSubWhenNatsGoesDown(c *C) {
  adapter, nats_cmd := before()

  messagesReceived := make([]string, 0)
  receivedChan := make(chan bool, 1)
  adapter.Subscribe("some-message", func (payload []byte) {
    messagesReceived = append(messagesReceived, string(payload))
    receivedChan <- true
  })
  
  StopNats(nats_cmd)
  waitUntilNatsDown(adapter.port)
  nats_cmd = StartNats(adapter.port)
  defer after(nats_cmd)

  adapter.Publish("some-message", []byte("This is a message"))

  failOnTimeout(receivedChan, 2 * time.Second, func() {
    c.Check(len(messagesReceived), Equals, 1)
    c.Check(messagesReceived[0], Equals, "This is a message")
  })

  adapter.UnsubscribeAll()

  adapter.Publish("some-message", []byte("message 2"))
  failOnEvent(receivedChan, 1 * time.Second, func(){})

  c.Check(len(messagesReceived), Equals, 1)
}

func (s *AdaptersSuite) TestRequest(c *C) {
  adapter, nats_cmd := before()
  defer after(nats_cmd)

  request_subject := "request_subject"
  subscribeBarrier := make(chan bool, 1)

  go NatsRequestResponder(adapter, request_subject, subscribeBarrier)

  messagesReceived := make([]string, 0)
  receivedChan := make(chan bool, 1)
  callback := func(response []byte) {
    messagesReceived = append(messagesReceived, string(response))
    receivedChan <- true
  }

  <-subscribeBarrier
  adapter.Request(request_subject, []byte("request"), callback)

  failOnTimeout(receivedChan, 1 * time.Second, func() {
    c.Check(len(messagesReceived), Equals, 1)
    c.Check(messagesReceived[0], Equals, "Response")
  })
}

func (s *AdaptersSuite) TestRespondToChannel(c *C) {
  requestAdapter, cmd := before()
  defer after(cmd)

  channel := "request-chan"

  respondAdapter := NewNatsAdapter()
  respondAdapter.Configure(requestAdapter.host, requestAdapter.port, requestAdapter.user, requestAdapter.password)
  respondAdapter.Connect()

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

  failOnTimeout(responseChannel, 2 * time.Second, func(){})
}

func (s *AdaptersSuite) TestRespondToChannelWithNoConnection(c *C) {
  respondAdapter := NewNatsAdapter()
  respondAdapter.Configure("127.0.0.1", 4223, "nats", "nats")

  channel := "request-chan"

  err := respondAdapter.RespondToChannel(channel, func(req []byte) []byte {
    req_string := string(req)
    res_string := strings.ToLower(req_string)
    return []byte(res_string)
  })
  c.Assert(err, Not(IsNil))

  requestAdapter, cmd := before()
  defer after(cmd)

  respondAdapter.Connect()
  
  request := "HELLO"
  responseChannel := make(chan bool, 1)

  requestAdapter.Request(channel, []byte(request), func(response []byte) {
    c.Assert(string(response), Equals, "hello")
    responseChannel <- true
  })

  failOnTimeout(responseChannel, 2 * time.Second, func(){})
}
