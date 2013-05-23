package go_cfmessagebus

import (
	. "launchpad.net/gocheck"
	"testing"
)

func Test(t *testing.T) { TestingT(t) }

type CFMessageBusSuite struct{}

var _ = Suite(&CFMessageBusSuite{})

func (s *CFMessageBusSuite) TestNothing(c *C) {
	c.Check(1, Equals, 1)
}
