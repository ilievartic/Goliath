# Goliath

### Authors

Manikandan Swaminathan, Logan Pulley, Deepan Venkatesh, Ilie Vartic, Zachary Oldham

### Abstract

This package enables Python to offload sets of function calls to pools of remote worker processes.

### Details

When handling large sets of data, the thread-pool model can often do wonders for parallelizing and thus speeding up a program. However, Python's native support for concurrency is more like _polling_ than _threading_; it doesn't properly take advantage of multiple CPU cores. This can be frustrating when working in Python with a task that would be easily threadable in other languages.

Goliath enables Python to distribute a set of function calls over a set of servers. This essentially simulates the thread-pool model as a pool of servers, each maintaining a pool of Python worker processes. Additionally, these servers can be reached over the Internet, enabling a many-to-many relationship between clients requesting work and servers providing workers; each client can have work distributed across multiple servers, and each server can handle work from multiple clients.

Goliath abstracts this entire model and aggregates the results from the servers, finally returning the list of results to the coder.

## Requirements

- Python 3.8

## Installation

Install with `pip`:

`pip install goliath`

## Usage

### Sending work (Commander)

```py
# foo.py

from goliath.commander import Commander

# Create a commander (doesn't connect yet)
cmdr = Commander([
    # Lieutenants can be hostnames, domains, IPs
    ('lieutenant-hostname', 8080),
    ('lieutenant.com', 3333),
    ('127.0.0.1', 2222)
])

# The function to execute
def foo(bar, baz):
    return str(bar) + str(baz)

# Function to generate list of arguments to try
def foo_args(bar_range, baz_range):
    for bar in bar_range:
        for baz in baz_range:
            yield { 'bar': bar, 'baz': baz }

# Connect to lieutenants, run all the functions, and get results
results = cmdr.run(foo, foo_args(range(100), range(100)), ['foo.py'])
```

### Performing work (Lieutenant & Worker)

To run a lieutenant on this machine on port 3333 with 8 worker processes:

`python3.8 -m goliath.lieutenant localhost 3333 8`

## Licensing

Goliath is open-source software, licensed under GNU's Lesser GPL.
