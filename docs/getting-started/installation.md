# Installation

## Go Install

Requires Go 1.23 or later.

```bash
go install github.com/mtingers/dflockd/cmd/dflockd@latest
```

This places the `dflockd` binary in your `$GOBIN` (usually `~/go/bin`).

## Build from Source

```bash
git clone https://github.com/mtingers/dflockd.git
cd dflockd
go build -o dflockd ./cmd/dflockd
```

To embed a version string:

```bash
go build -ldflags "-X main.version=1.0.0" -o dflockd ./cmd/dflockd
```

## Docker

```bash
docker build -t dflockd .
docker run -p 6388:6388 dflockd
```

## Go Client Library

To use dflockd from a Go application, add the client package:

```bash
go get github.com/mtingers/dflockd/client
```

## Verify

```bash
dflockd --version
```
