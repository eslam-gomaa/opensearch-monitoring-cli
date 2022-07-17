# opcli

A CLI tool that provides the data we need for optimizing opensearch performance

<br>

The following functions are supported:
- [List Nodes](#list-nodes)
- [Live monitoring for Nodes](#node-top)
- [List Indices](#list-indices)
- [live monitoring for Indices](#top-index)
- [show shards allocation over the nodes](#shards-allocation) (for a giving Index pattern)


# Installation

> At least Python 3.6 is needed

```
pip3 install opmcli
```

---

<br>

If you see a similar warning:
```
WARNING: The script opmcli is installed in '/Users/YOU/Library/Python/3.8/bin' which is not on PATH.
  Consider adding this directory to PATH or, if you prefer to suppress this warning, use --no-warn-script-location.
```

You need to create an alias ( _Prefered to put it in `.bashrc`_ )

_Example:_

```
alias opmcli="/Users/YOU/Library/Python/3.8/bin/opmcli"
```

<br>

<details>
    <summary>
        <b style="font-size:17px"> <code>Build from source</code></b> [ optional ]
    </summary>
    <br>

``` bash
python setup.py sdist bdist_wheel

mv dist/opmcli-0.0.1-py2-none-any.whl dist/opmcli-0.0.1-py36-none-any.whl

pip3 install dist/opmcli-0.0.1-py36-none-any.whl
```

<br>  
</details>

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

| ENV                       | Required | Default value |
| --------------------------- | ---------- | --------------- |
| `OPENSEARCH_ENDPOINT`   | YES      |               |
| `OPENSEARCH_PORT`       | YES      | 443           |
| `OPENSEARCH_BASIC_AUTH` | NO       | no            |
| `OPENSEARCH_USERNAME`   | NO       |               |
| `OPENSEARCH_PASSWORD`   | NO       |               |


---

<br>

# Examples


#### List Indices (with shards details)
<a id=list-indices></a>


```
opmcli --list --index INDEX_PATTERN
```

<br>

#### List Nodes
<a id=list-nodes></a>


```
opmcli --list --nodes
```

<br>

#### Live monitoring for a Node
<a id=node-top></a>


```
opmcli --top --node NODE_ID
```

**Monitored Metrics:**
- [x] cpu
- [x] memory
- [x] swap
- [x] fs
- [x] File_descriptors
- [x] Disk IO
- [x] JVM
- [x] Indexing Rate
- [x] Indexing Latency
- [x] Searching Rate
- [x] Searching Latency
- [x] Fetch Rate
- [x] Fetch Latency
- [ ] Refresh Rate
- [ ] Refresh Latency
- [ ] Field data
- [ ] Threads

> More to be added upon need



<br>

#### Live monitoring for an Index / Indices
<a id=top-index></a>


```
opmcli --top --index INDEX_PATTERN
```

**Monitored Metrics:**
- [x] Store size
- [x] documents count
- [x] Indexing Rate
- [x] Indexing Latency
- [x] Searching Rate
- [x] Searching Latency
- [x] Fetch Rate
- [x] Fetch Latency
- [x] Refresh Rate
- [x] Refresh Latency
- [ ] Field data

> More to be added upon need


<br>

#### Print shards allocation across the nodes
<a id=shards-allocation></a>


```
opmcli --list --index INDEX_PATTERN --display-shards
```
