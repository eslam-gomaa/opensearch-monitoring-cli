# opcli

A CLI tool that provides the data we need for optimizing opensearch performance

<br>

The following functions are supported:
- List Nodes
- Live monitoring for Nodes
- List Indices
- live monitoring for Indices
- show shards allocation on the nodes (for a giving Index pattern)


# Installation


```

```

---

<br>


```bash
WARNING: The script opmcli is installed in '/Users/YOU/Library/Python/3.8/bin' which is not on PATH.
  Consider adding this directory to PATH or, if you prefer to suppress this warning, use --no-warn-script-location.
```

You need to create an alias
```
alias opmcli="/Users/YOU/Library/Python/3.8/bin/opmcli"
```
> It's prefered to put it in `.bashrc`


## Build from source (optional)

``` bash
python setup.py sdist bdist_wheel

mv dist/opmcli-0.0.1-py2-none-any.whl dist/opmcli-0.0.1-py36-none-any.whl

pip3 install dist/opmcli-0.0.1-py36-none-any.whl

# pip3 install -e .

```

<br>

---

<br>

# Authentication

You need to provide the following Environment variables

```bash
export OPENSEARCH_ENDPOINT=
export OPENSEARCH_PORT=443
export OPENSEARCH_BASIC_AUTH=yes
export OPENSEARCH_USERNAME=
export OPENSEARCH_PASSWORD=
```

---

<br>

# Examples


List Indices (with shards details)

```
python3 opmcli.py --list --index INDEX_PATTERN
```

<br>

List Nodes

```
python3 opmcli.py  --list --nodes
```

<br>

Live monitoring for a Node

```
python3 opmcli.py --top --node NODE_ID
```

<br>

Live monitoring for an Index / Indices

```
python3 opmcli.py --top --index INDEX_PATTERN
```


<br>

Print shards allocation across the nodes

```
python3 opmcli.py --list --index fcdr-2022-* --display-shards
```