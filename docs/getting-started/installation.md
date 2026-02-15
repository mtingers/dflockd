# Installation

## Requirements

- Go 1.23+ (for building from source)

## Install with `go install`

```bash
go install github.com/mtingers/dflockd/cmd/dflockd@latest
```

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
INFO dflockd: listening on ('0.0.0.0', 6388)
```
