package cfmessagebus

import (
	. "launchpad.net/gocheck"
	"testing"
)

func Test(t *testing.T) { TestingT(t) }

type MessageBusSuite struct{}

var _ = Suite(&MessageBusSuite{})

func (s *MessageBusSuite) TestNothing(c *C) {
	c.Check(1, Equals, 1)
}
