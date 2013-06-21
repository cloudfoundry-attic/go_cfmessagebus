[![Build Status](https://travis-ci.org/cloudfoundry/go_cfmessagebus.png)](https://travis-ci.org/cloudfoundry/go_cfmessagebus)

# Go CF Message Bus

This repository contains the source code for the Go implementation of a message bus wrapper for Cloud Foundry components.

# Goal

* Abstract the actual message bus implementation away from the internals of the rest of the codebase.
* Allow changing the message bus being used without needing to rewrite the rest of the Go components.