package go_cfmessagebus

import (
  "fmt"
  nats "github.com/apcera/nats"
  "net"
  "errors"
)

type NatsAdapter struct {
  client *nats.Conn
  host string
  user string
  port int
  password string
  subscriptions []*Subscription
}

type Subscription struct {
  subject string
  callback func([]byte)
  reply func([]byte) []byte
  natsSubscription *nats.Subscription
}

func NewNatsAdapter() *NatsAdapter {
  return &NatsAdapter{}
}

func(adapter *NatsAdapter) Configure(host string, port int, user string, password string) {
  adapter.host = host
  adapter.port = port
  adapter.user = user
  adapter.password = password
}

func(adapter *NatsAdapter) Connect() error {

  user_password := ""
  if adapter.user != "" || adapter.password != "" {
    user_password = fmt.Sprintf("%s:%s@", adapter.user, adapter.password)
  }

  url := fmt.Sprintf("nats://%s%s:%d", user_password, adapter.host, adapter.port)
  natsClient, err := nats.Connect(url)
  if err != nil {
    return err
  }

  adapter.client = natsClient

  for _, sub := range adapter.subscriptions {
    subscribeInNats(adapter, sub)
  }

  return nil
}

func(adapter *NatsAdapter) Subscribe(subject string, callback func(payload []byte)) error {
  sub := &Subscription{subject: subject, callback: callback}
  adapter.subscriptions = append(adapter.subscriptions, sub)
  
  if adapter.client != nil {
    subscribeInNats(adapter, sub)
  } else {
    return errors.New("No connection to Nats. Caching subscription...")
  }
  return nil
}

func(adapter *NatsAdapter) UnsubscribeAll() error {
  return withConnectionCheck(adapter.client, func () {
    for _, sub := range adapter.subscriptions {
      sub.natsSubscription.Unsubscribe()
    }
  })
}

func(adapter *NatsAdapter) Publish(subject string, message []byte) error {
  return withConnectionCheck(adapter.client, func () {
    adapter.client.Publish(subject, message)
  })
}

func(adapter *NatsAdapter) Request(subject string, message []byte, callback func(payload []byte)) error {
  return withConnectionCheck(adapter.client, func() {
    inbox := nats.NewInbox()
    msg := &nats.Msg{
      Subject: subject,
      Reply: inbox,
      Data: message,
    }

    adapter.client.Subscribe(inbox, func(msg *nats.Msg) {
      callback(msg.Data)
      msg.Sub.Unsubscribe()
    })

    adapter.client.PublishMsg(msg)
  })
}

func(adapter *NatsAdapter) RespondToChannel(subject string, replyCallback func([]byte) []byte) error {
  sub := &Subscription{subject: subject, reply: replyCallback}
  adapter.subscriptions = append(adapter.subscriptions, sub)

  if adapter.client != nil {
    subscribeInNats(adapter, sub)
  } else {
    return errors.New("No connection to Nats. Caching subscription...")
  }
  return nil
}

func(adapter *NatsAdapter) Ping() bool {
  _, err := net.Dial("tcp", fmt.Sprintf("%s:%d", adapter.host, adapter.port))
  return err == nil
}

func withConnectionCheck(connection *nats.Conn, callback func()) error {
  if connection == nil {
    return errors.New("No connection to Nats")
  }
  callback()
  return nil
}

func subscribeInNats(adapter *NatsAdapter, sub *Subscription) {
  natsSub, _ := adapter.client.Subscribe(sub.subject, func(msg *nats.Msg) {
    if sub.reply != nil {
      respondSubject := msg.Reply
      response := sub.reply(msg.Data)
      adapter.client.Publish(respondSubject, response)
    } else {
      sub.callback(msg.Data)
    }
  })
  sub.natsSubscription = natsSub
}
