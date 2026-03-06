# Installation

## Pre-built binaries

Download a pre-built binary from the [GitHub Releases](https://github.com/mtingers/dflockd/releases) page. Binaries are available for Linux, macOS, and Windows on amd64 and arm64.

```bash
# Example: download and extract the latest release for Linux amd64
curl -Lo dflockd.tar.gz https://github.com/mtingers/dflockd/releases/latest/download/dflockd_Linux_amd64.tar.gz
tar xzf dflockd.tar.gz
./dflockd
```

## Requirements

- Go 1.23+ (for building from source or `go install`)

## Install with `go install`

```bash
go install github.com/mtingers/dflockd/cmd/dflockd@latest
```

See [GOPATH documentation](https://go.dev/wiki/GOPATH) for more information
on how this works.

## Build from source

```bash
git clone https://github.com/mtingers/dflockd.git
cd dflockd
make build
```

## Verify installation

Start the server to confirm everything is working:

```bash
# If installed with go install
dflockd

# If built from source
./dflockd
```

You should see log output indicating the server is listening:

```
time=... level=INFO msg=listening addr=127.0.0.1:6388
```
