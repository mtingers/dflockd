# Installation

dflockd is a single Go binary. Build from source or `go install`.

## Prerequisites

- Go 1.23 or newer (`go.mod` declares `go 1.23` and a `1.26.2`
  toolchain). Earlier releases work for the client, not for the
  server.

## go install

```bash
go install github.com/mtingers/dflockd/cmd/dflockd@latest
```

Drops the `dflockd` binary into `$GOBIN` (typically `~/go/bin`).

## Build from source

```bash
git clone https://github.com/mtingers/dflockd
cd dflockd
make build      # → ./dflockd
```

`make build` injects the version string from the `VERSION` make
variable; `make build VERSION=v2.0.0-rc1` is the typical invocation
for a release.

## Verify

```bash
./dflockd --version
./dflockd --help        # full flag listing
```

## Client library

The Go client is a separate import:

```bash
go get github.com/mtingers/dflockd/client
```

See the [Go client docs](../client.md) for the API.

## What's not in this binary

- The benchmark tool lives at `./cmd/bench` — `go run ./cmd/bench
  --help` from a checkout. It's a development utility, not part of
  the release.
- The complexity reporter is at `./tools/complexity` — also a
  development utility.
