# Transport Final Design — Unified Rent-and-Receive, Channel Outbound

**Date:** 2026-07-06
**Status:** Approved (design), pending implementation plan
**Scope:** `src/Servus.Akka/Transport/**` + pooling/IO support; lockstep migration of the GaudiHTTP consumer
**Baseline:** branch `feat/zero-copy-inbound-tcp` @ `9b511dc` (97 commits vs `main`)

## Context

The branch went through two zero-copy attempts. Attempt #1 (`PipeSegmentLease`/`LeaseTracker`, pull-based `PipeTransportStage`) was deleted — leasing pipe segments across the stage boundary was too complex to keep correct. Attempt #2 (rent-and-receive: socket receives directly into a rented buffer whose ownership travels downstream) works: the per-read memcpy is gone, the input pipe is deleted, and backpressure via "one outstanding read + kernel TCP window" is stricter than the old pipe thresholds.

This design consolidates the transport on that model end to end: one inbound discipline for TCP/TLS/QUIC, an ownership-based outbound path replacing the output pipe, one buffer type, one pool story, and the accumulated correctness fixes. Breaking the public transport API is allowed; GaudiHTTP is the only consumer and migrates in the same change.

### Blockers this design resolves (from the 2026-07-06 branch review)

1. `_readInProgress` reset before the generation check in both TCP state machines → stale event enables a second concurrent `ReceiveAsync` → `SocketAwaitable` VTS corruption.
2. Pooled-lease reuse with an in-flight receive → VTS corruption plus permanent byte loss (stale-gen handler disposes the buffer that carried live bytes).
3. `ReceiveAsync` non-reentrancy is documentation-only — silent corruption instead of a diagnosable failure.

## Decisions (locked)

| Axis | Decision |
|------|----------|
| Scope | Full target architecture, not blockers-only |
| Lease reuse with pending receive | Quiesce: cancellable receive, `Release` cancels + awaits before pooling |
| QUIC inbound | Delete all pipe-based reads (`CreateWithStreamReader`, `StreamPipeReader` path); rent-and-receive everywhere |
| Outbound | Bounded queue of owned buffers replaces the output pipe; vectored send; watermark backpressure |
| API compatibility | Breaking allowed (pre-1.0, single consumer pinned via submodule) |

## 1. Component structure

`SocketPipeConnection` (four modes encoded in nullable-field combinations across three constructors) is replaced by two concrete types behind one interface:

```
IDuplexConnection
├── RawSocketConnection    — plaintext TCP: SocketAwaitable receive, vectored SendAsync
└── StreamConnection       — everything Stream-shaped: SslStream (TLS), QuicStream, test streams
```

```csharp
public interface IDuplexConnection : IAsyncDisposable
{
    ValueTask<WireBuffer?> ReceiveAsync(CancellationToken ct);  // null = EOF; owned buffer on data
    bool TryEnqueue(WireBuffer buffer);                         // outbound; ownership transfers.
                                                                // false only when the connection is
                                                                // quiesced/disposed (terminal) — the
                                                                // queue itself is not size-bounded;
                                                                // the SM watermark discipline bounds
                                                                // bytes in flight (see section 3)
    ValueTask QuiesceAsync();                                   // cancel pending receive, await it
}
```

- The interface is the seam state machines and tests program against. `CreateInert` and its input-pipe machinery are deleted; state-machine tests use a fake `IDuplexConnection`.
- `StreamConnection` gets a QUIC-aware error-mapping hook (`QuicException` → graceful close, not fault), so `QuicStreamState` needs no read plumbing of its own.
- Deleted with the pipe path: `PipeStreamReadResult`, `PendingAdvance`, `_cachedReader`, `InputReader`, the inert constructor, and `IOQueue`'s role as input scheduler (it remains only if the send loops still want it; otherwise deleted too — decided during implementation by measurement, default is delete).

## 2. Inbound path (TCP, TLS, QUIC — one discipline)

Pull-gated rent-and-receive:

1. Stage `onPull` → SM `RequestRead()`, guarded by `_readInProgress`. **The flag is cleared only inside the generation check** in `ReadCompleted`/`ReadFailed` handling (blocker fix 1). `ReceiveAsync` additionally carries an `Interlocked` reentrancy guard that throws on concurrent entry (blocker fix 3).
2. `ReceiveAsync`: zero-byte probe first — this is now the only mode; the `WaitForData` option is deleted and idle connections pin no buffer — then rent a `WireBuffer` at the adaptive hint (4K–128K; one shared `AdaptHint` helper replaces the two divergent copies), receive directly into it, transfer ownership to the completion.
3. Sync fast-path with budget 8 kept on client and server, TCP and QUIC alike. The async path uses **cached PipeTo transforms**: a small per-generation read-state object owns the success/failure delegates, so per-read allocation drops to the boxed event + Akka envelope.
4. **QUIC streams become pull-gated like TCP**: `RequestStreamRead` re-arms only on stage demand; the unbounded `_pendingReads` queue is removed. The ~250 duplicated lines of read-completion handling across `QuicTransportStateMachine`/`QuicServerStateMachine` collapse into one shared handler, since the only read result shape left is `WireBuffer?`.

Backpressure: exactly one outstanding receive per connection/stream, at most one un-pushed element in the stage; everything else backs up in the kernel (TCP window / QUIC flow control). No in-process thresholds; all `Input*Threshold` options are deleted.

## 3. Outbound path

The output pipe is replaced by a bounded outbound queue of owned `WireBuffer`s per connection (and per QUIC stream) plus the send loop:

1. SM extracts the buffer from `TransportData`, calls `data.Return()` (fixing the missing return in `TcpConnectionStateMachine.HandleTransportData`), then `TryEnqueue`s the buffer — no copy. The SM tracks bytes in flight: above a high-watermark it stops pulling upstream, below the low-watermark it resumes. This replaces `PipeFlushComplete`/`_needsFlush`/`_pendingWrites`; client and server share one outbound discipline.
2. Send loop drains the queue per cycle:
   - `RawSocketConnection`: vectored `SendAsync` over the drained batch — multi-buffer coalescing with zero copy.
   - `StreamConnection`: small buffers are coalesced into one rented buffer per drain (single copy, only when buffers are small; threshold tuned during implementation, initial value 4 KB per buffer); large buffers go as sequential `WriteAsync` with zero copy. Replaces today's two mandatory copies.
3. After each drain the loop disposes the sent buffers and posts **one coalesced "flushed N bytes" message** to the SM actor — the shape the GaudiHTTP `TransportFlushed` outbound-credit design consumes.
4. QUIC batching asymmetry ends: `ConflateWithSeed` (per-cycle `List<>` alloc) is removed; the per-stream queue is the batching. `FlushBatch`/`_dirtyStreams` reduces to signalling dirty streams' send loops, identical on client and server.

## 4. Buffer model & pooling

- `TransportBuffer` + `PooledArrayMemoryOwner` merge into **`WireBuffer`**: a pooled wrapper (`Interlocked` dispose guard; `Offset`/`Length`/`Memory`; `IMemoryOwner<byte>`) over an array from the existing cross-thread `SharedPool`. GaudiHTTP's `TransportBuffer.Offset`/`Wrap` usage migrates to `WireBuffer`.
- One wrapper pool, actually configurable, default 1024 slots (sized for real in-flight counts under H2/H3 multiplexing, not `CPU×4`). The no-op `ConfigurePoolSize` knob is deleted. The implicit `byte[]` conversion (copy-alloc trap) is deleted.
- `TransportData`/`MultiplexedData` remain the pooled stage-boundary carriers (256 each).
- Steady-state per read: 0 copies inbound, 0–1 copies outbound (0 on plaintext TCP), 2 pooled objects (`WireBuffer` + carrier), 1 boxed event + envelope on the async path.

## 5. Lifecycle & error handling

`QuiesceAsync()` cancels the pending receive and awaits its completion; a cancelled `ReceiveAsync` disposes any rent it holds (usually none — the parked operation is the zero-byte probe). Three call sites:

1. **Lease release into the pool** (blocker fix 2): `Release` quiesces before the lease becomes acquirable — no in-flight operation crosses lease boundaries, warm-connection (LIFO) reuse stays.
2. **Reconnect / `CleanupTransport`**: quiesce, then bump the generation. The gen check remains the second line of defense for the now-narrow stale-event window.
3. **Stage `PostStop`**: quiesce before dispose — closes the dead-letter buffer leak (no data-carrying completion can be in flight toward a dead actor).

Teardown order everywhere: quiesce receive → dispose queued outbound buffers (the connection owns pending-write disposal) → graceful FIN close (kept).

Folded-in fixes:

- `HandleUpstreamFinish` re-checks completion when the flushed-ack arrives (fixes the shutdown hang when a flush is in flight).
- `QuicTransportStateMachine.HandleUpstreamFinish` disposes streams and returns the lease (today only `PostStop` does).
- QUIC server accept loop adopts the client's terminal-null semantics; `QuicConnectionHandle` stops swallowing exceptions into null (ends the busy-spin on dead connections).
- Send-loop flush results are always observed (no discarded `ValueTask<FlushResult>` equivalent in the new loops).
- QUIC server gets the same sync-read budget as the client; `MigrationCheck` becomes a shared helper.

## 6. Configuration cleanup

- Deleted: `InputPauseWriterThreshold`/`InputResumeWriterThreshold` (`SocketPipeConnectionOptions`), `InputPauseThreshold`/`InputResumeThreshold` (`TransportOptions`, `ListenerOptions`), `WaitForData`, `TransportBuffer.ConfigurePoolSize`.
- Wired: `ReceiveBufferHint` reaches the stream/QUIC paths; `TcpPoolConfig.IdleTimeout` and `QuicTransportOptions.ConnectionLifetime` are honored by eviction (no more hardcoded 10 minutes).
- New: outbound high/low watermark (bytes) and `WireBuffer` pool size on `TransportOptions`.

## 7. Testing & validation

New unit specs:

- Quiesce-then-reuse: release with pending receive → next acquirer reads cleanly, no corruption, no byte loss.
- Stale-gen `_readInProgress` race: stale `ReadCompleted` while a current-gen read is in flight → no second `ReceiveAsync`.
- Reentrancy guard: concurrent `ReceiveAsync` throws.
- Watermark backpressure: pulls stop above high-water, resume below low-water; flushed-ack coalescing.
- QUIC pull-gating: pending reads bounded under slow consumer.
- Accept-loop terminal null: no busy-spin on dead server connection.
- Quiesce-on-PostStop: no buffer leak to dead letters.

Consumer validation: full GaudiHTTP unit+stage suite plus all three integration suites after the lockstep migration.

**Benchmark gate** (before this replaces the branch): server upload suite — the one axis where losing receive/processing overlap could regress — measured against both the current branch and the pre-branch baseline; H3 upload allocation and client upload allocation (expected to improve materially); plaintext throughput as the no-regression canary.

## Out of scope

- The GaudiHTTP-side `TransportFlushed`/outbound-credit protocol redesign itself (this design only produces the coalesced flushed-ack it consumes).
- H2 batch split (~256 KB) and credit reset on reconnect (prerequisites of that separate design).
- Any protocol-layer (H1/H2/H3 state machine) changes in GaudiHTTP beyond the mechanical `WireBuffer` migration.
