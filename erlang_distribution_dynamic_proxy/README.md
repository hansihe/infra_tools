# Erlang Distribution Dynamic Proxy
This is a CLI tool which will let you easily connect to a Erlang node
running on a kubernetes cluster over the Erlang distribution protocol.

In order to make this low friction, it uses a fairly nasty hack. More
about this in the bottom of the README, see "THE HACK".

WARNING: Due to "THE HACK", the node name you see in your local session
when connecting to a remote are not accurate, and any mentions of the
node names going over the distribution protocol will be spookily
rewritten.

## Installation
1. Make sure you have `cargo` installed. It can be installed from [rustup.rs](rustup.rs).
2. Run `cargo install erlang-distribution-dynamic-proxy --git https://github.com/hansihe/infra_tools.git`

## Usage

```
erlang-distribution-dynamic-proxy --namespace <k8s_ns> proxy-deployment <k8s_deployment>
```

The remote node needs to have erlang distribution enabled for this to
work. It will attempt to read the cookie from the following env variables
in the deployent by default (in order):
* `RELEASE_COOKIE`
* `ERLANG_COOKIE`
* `K8S_NODE_COOKIE`
* `REMSH_COOKIE`

Another env var name can be specified using CLI flags, see
`erlang-distribution-dynamic-proxy proxy-deployment --help` for more info.

## How it works

When you run the `proxy-distribution` subcommand, the tool will:
* Resolve kubernetes context from the shell environment
* Fetch the deployment specified
* Find the cookie set in the deployment configuration
* Locate a pod in the deployment
* Connect to the `epmd` daemon in the target pod to read metadata
* Connect to the `epmd` daemon on localhost, register proxied node
* Listen on localhost for erlang distribution connections

At this point you will be able to connect to the remote node as
if it was on your local machine. On an incoming erlang distribution
connection, the proxy will:
* Begin the dist handshake with the incoming connection
* Begin the dist handshake with the outgoing connection
* During handshake, it will rewrite some of the packets to make
  your local node believe it is connecting to a node running on
  localhost.
* Finish the dist handshake with both parties
* Forward erlang distribution packets in both directions, decoding
  and reencoding them while performing THE HACK

### THE HACK
The Erlang distribution protocol is very strict about node names
matching when connecting.

If you have a node with the name `app@10.3.2.1`, and you attempt
to `Node.connect(:"app@127.0.0.1")`, this will not succeed as
several layers in the erlang distribution protocol stack assumes
these are identical.

This makes it tricky when Erlang nodes are running in a cluster
on remote machines.

To solve this, the proxy simply rewrites any occurrences of the
true name (`app@10.3.2.1`) to what your local node actually
connected to (`app@127.0.0.1`) and back.

This is a nasty hack, but it works rather well for debugging and
profiling purposes. You always have to keep in mind that node
names in your local shell / livebook may not match what the remote
node actually is named.
