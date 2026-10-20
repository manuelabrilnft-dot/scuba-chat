# Scuba Chat

A peer to peer chat application for an ad hoc wireless network. Every node is
equal. There is no server, no router and no base station. Nodes find each other,
work out who can reach whom, and forward messages for each other so that two
nodes that cannot hear one another can still talk.

Built in Java for the Network Systems module at the University of Twente. The
nodes share one emulated radio channel, so every transmission is heard by
everyone in range and two nodes transmitting at once destroy each other's
packets. Most of the work went into handling that.

## The problem

The channel gives you very little to work with:

- One shared half duplex medium, so collisions are real and must be handled
- Up to 8 nodes
- A 2 byte header budget per packet, so every field is counted in bits
- Small payloads, so anything longer has to be split and put back together
- Nodes join and leave at any time, and nobody is told when

## What it implements

**Address assignment.** A node joining the network picks a random id and
announces it. If two nodes pick the same id the clash is detected and one of
them picks again. Ids in use are tracked so a newcomer does not collide with a
node that is already settled.

**Neighbour discovery.** Each node sends a heartbeat carrying a bitmap of the
nodes it currently knows about. Merging those bitmaps gives every node a picture
of the network without any node needing to see all of it directly. A node that
stops sending heartbeats is timed out and removed, and the bitmap is updated.

**Token passing MAC.** Only the node holding the token may transmit, which keeps
the shared channel collision free during normal operation. The token is passed
around the known nodes in turn. A node with nothing to send passes it on
straight away. If a token is lost the holder times out and the token is
regenerated, and token handoffs are acknowledged and retransmitted.

**Multi hop routing.** Messages carry a next hop alongside the final
destination. A node that is not the destination forwards the packet to the next
hop on the path, so traffic reaches nodes that are out of direct range.

**Fragmentation and reassembly.** Long messages are split across packets with a
sequence number and a last fragment flag. The receiver buffers fragments per
source, orders them by sequence number and reassembles the message.

**Acknowledgements.** Payload packets are acknowledged. An unacknowledged packet
is retransmitted after a timeout.

## Packet format

Both header types are 16 bits. The top bit says which one it is.

**Payload, top bit 0**

| Bits | Field |
| --- | --- |
| 14-12 | source id |
| 11-9 | destination id |
| 8 | ack flag |
| 7 | last fragment flag |
| 6-4 | next hop |
| 3-0 | sequence number |

**Control, top bit 1**

| Bits | Field |
| --- | --- |
| 14-12 | source id |
| 11-8 | nonce, `0xF` marks a token |

A heartbeat then carries a collision flag and a 7 bit bitmap of known nodes. A
token carries the next hop and the destination.

## How it is built

`MyProtocol` holds the state machine and runs the work across several threads:

| Thread | Does |
| --- | --- |
| `TokenThread` | holds and passes the token |
| `TimeoutTokenThread` | regenerates a lost token |
| `EliberateTokenThread` | releases the token when there is nothing to send |
| `HeartbeatThread` | sends heartbeats on a timer |
| `HeartbeatHistoryThread` | times out nodes that went quiet |
| `LockThread` | guards an id during address assignment |
| `pushToSendingQueueThread` | queues outgoing packets and tracks ack timeouts |
| `receiveThread` | parses incoming packets, reassembles and acks |

State is shared through blocking queues and concurrent maps rather than locks
around the protocol logic.

```
src/
  MyProtocol.java      state machine and all protocol threads
  Header.java          header interface and the parser
  PayloadHeader.java   data packets
  HeartbeatHeader.java neighbour discovery
  TokenHeader.java     token passing
  Utils.java           timeout helper
  client/              connection to the emulated radio and the terminal chat
```

## Running it

Needs a JDK and access to the university network emulator.

```
make            # compiles src into bin
make run        # runs the node
make clean      # removes class files
```

The node connects to `netsys.ewi.utwente.nl:8954` on frequency 2600. Change
`SERVER_IP`, `SERVER_PORT` and `frequency` at the top of `MyProtocol.java` to
point somewhere else. Start one node per terminal, type a message and press
enter to send it.

## Known issues

Found by reading the code. The emulator is only reachable from inside the
university network, so these are not all confirmed by running it.

**Token header fields parse wrong.** In `Header.parse` the next hop and
destination are read as `header & 0b11100000 >>> 5`. In Java `>>>` binds tighter
than `&`, so this shifts the mask instead of the value and both fields come out
as the low three bits. The mask needs its own brackets.

**`TokenHeader` drops the nonce.** The constructor takes a nonce and never
assigns it, so `getNonce` returns 0 on a parsed token.

**Debug output is left in.** `HeartbeatHeader.assemble` prints the raw header on
every call, which fills the terminal during a chat.

**There are two copies of the source.** `src/` is what the Makefile builds.
`CustomNetworkProtocol/` is an older copy that has drifted and is not built by
anything.

## License

MIT. See `LICENSE`.
