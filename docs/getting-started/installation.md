# Installation

## Requirements

- Python 3.13 or later
- No external dependencies

## Install from PyPI

=== "pip"

    ```bash
    pip install dflockd
    ```

=== "uv"

    ```bash
    uv add dflockd
    ```

## Install from source

```bash
git clone https://github.com/mtingers/dflockd.git
cd dflockd
uv sync
```

## Verify installation

Start the server to confirm everything is working:

```bash
# If installed from PyPI
dflockd

# If installed from source
uv run dflockd
```

You should see log output indicating the server is listening:

```
INFO dflockd: listening on ('0.0.0.0', 6388)
```
