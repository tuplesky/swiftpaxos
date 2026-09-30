SwiftPaxos: Fast Geo-Replicated State Machines
==========
<!-- [![Go Report Card](https://goreportcard.com/badge/github.com/imdea-software/swiftpaxos)](https://goreportcard.com/report/github.com/imdea-software/swiftpaxos) -->

This repository contains the prototype implementation of SwiftPaxos, a new state-machine replication protocol for geo-distributed systems.
SwiftPaxos is a _faster Paxos without compromises_.
In the best case, it executes a state-machine command in two message delays (one round-trip), and three otherwise.
SwiftPaxos was [presented](https://www.usenix.org/conference/nsdi24/presentation/ryabinin) at the 21st USENIX Symposium on Networked Systems Design and Implementation ([NSDI '24](https://www.usenix.org/conference/nsdi24)).

Installation
------------

    git clone https://github.com/imdea-software/swiftpaxos.git
    cd swiftpaxos
    go install github.com/imdea-software/swiftpaxos

Implemented protocols
---------------------

|  Protocol               | Comments                                          |
|-------------------------|---------------------------------------------------|
| SwiftPaxos              | See our NSDI'24 [paper](https://www.usenix.org/conference/nsdi24/presentation/ryabinin) for the full details.|
| Paxos                   | The classic Paxos protocol.                       |
| N<sup>2</sup>Paxos      | All-to-all variant of Paxos.                      |
| CURP                    | CURP implemented over N<sup>2</sup>Paxos.         |
| Fast Paxos              | Fast Paxos with uncoordinated collision recovery. |
| EPaxos                  | A [corrected][epaxos_correct] version of EPaxos.  |

This software is based on the [Egalitarian Paxos](https://github.com/otrack/epaxos) code base, as well as the corrections made [here](https://github.com/otrack/epaxos).

Usage
-----
#### participants
There are three types of participants: *master*, *servers* and *clients*. 
The servers and clients implement the protocol logic. 
The master maintains the configuration of the system.

#### deployment configuration
To setup a run, the participants read deployment configuration file. 
See [aws.conf][config] for an example of configuration file for AWS EC2.

#### launching a participant

Master:
    
    swiftpaxos -run master -config conf.conf

Server:

    swiftpaxos -run server -config conf.conf -alias server_name

Client:

    swiftpaxos -run client -config conf.conf -alias client_name

#### command line options

    -alias alias
        An alias of this participant
    -config file
        Deployment config file (required)
    -latency file
        Latency config file
    -log file
        Path to the log file
    -protocol protocol
        Protocol to run. Overwrites protocol field of the config file
    -quorum file
        Quorum config file
    -run participant
        Run a participant

See [quorum.conf][quorum] and [latency.conf][latency] for an example of quorum and latency configuration files.

Jepsen client
-------------

`cmd/swiftpaxos-jepsen` is one client session for a [Jepsen](https://jepsen.io) test, driven over JSON lines on stdin and stdout: one request a line in (`{"f":"write","key":3,"value":7}`, `{"f":"read","key":3}`), one answer a line out (`{"type":"ok","value":7}`).
It connects the SwiftPaxos client through the master, with a chosen replica as its closest.
An operation that times out is `info` for a write (it may still take effect) and `fail` for a read, and the session ends after it: the client keeps one reply value per session, so a late reply could be taken for the next command's.
The Jepsen test that uses it is `swiftpaxos/` in [tuplesky/jepsen](https://github.com/tuplesky/jepsen).

    go build -o bin/ . ./cmd/swiftpaxos-jepsen
    bin/swiftpaxos-jepsen -server n1:7070 -master 172.18.0.1 -replicas 5 -log client.log

`-server` takes `HOST:PORT` or `HOST`; the client matches replicas by host alone, and takes the closest by ping when none matches.

Two upstream problems show under Jepsen partitions, and this client does not fix them:

- The master's leader check (`master/master.go`, `Replica.Ping` in a sequential loop) is a `net/rpc` call with no deadline, so a leader cut off by a dropping partition hangs it and is never replaced.
- A replica flushes its peers' writers under the global `r.M` from one goroutine, with no write deadline (`replica/replica.go`, `replica/sender.go`, `swift/batcher.go`), so once the send buffer to a cut peer fills, the whole replica stalls.

Flint
-----

To have an idea on how different replication protocols would compare, we wrote a tool named [Flint][flint]. 
Flint takes as input a set of AWS regions.
It computes the expected latencies and estimates how the protocols perform in such a deployment.

[config]: aws.conf
[epaxos_correct]: https://github.com/otrack/on-epaxos-correctness
[quorum]: quorum.conf
[latency]: latency.conf
[flint]: https://github.com/vonaka/flint
