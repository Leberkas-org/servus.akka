# Transport Final Version Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Consolidate the servus.akka transport on unified pull-gated rent-and-receive inbound and an owned-buffer channel outbound, per the approved spec `docs/superpowers/specs/2026-07-06-transport-final-design.md`.

**Architecture:** Two connection types (`RawSocketConnection`, `StreamConnection`) behind `IDuplexConnection` replace the four-mode `SocketPipeConnection`. One buffer type (`WireBuffer`) replaces `TransportBuffer` + `PooledArrayMemoryOwner`. Outbound pipes are replaced by an SPSC channel of owned buffers with byte-watermark backpressure. `QuiesceAsync` (cancel pending receive + await) is the lifecycle primitive for lease reuse, reconnect, and stage stop.

**Tech Stack:** .NET 9/10, Akka.NET (GraphStage + stage actor PipeTo), System.Threading.Channels, xUnit v3 (`dotnet run`, not `dotnet test`).

**Repo:** `D:\GIT\Akka.Streams.Http\lib\servus.akka` unless a path starts with `GaudiHTTP` (then `D:\GIT\Akka.Streams.Http\src`). All `dotnet` commands for servus.akka run from `lib/servus.akka` (SDK pinned by its `global.json` — verify with `dotnet --version` in that directory first).

## Global Constraints

- Size literals: always `N * 1024` / `N * 1024 * 1024`, never raw numbers.
- No `volatile`; actor confinement is the threading model; `Interlocked` only at true cross-thread boundaries (pools, dispose guards).
- No `.Result` / `.Wait()` / `.GetAwaiter().GetResult()` / `async void`. Guarded `readTask.IsCompletedSuccessfully ? readTask.Result : await readTask` sync fast-paths are the one sanctioned `.Result` pattern (existing convention).
- Never `.AsTask()` before `PipeTo`; check `IsCompletedSuccessfully` for the sync fast-path first.
- Rent arrays only from the cross-thread `SharedPool` (locked buckets), never `ArrayPool<byte>.Shared` / `MemoryPool<byte>.Shared` on transport paths.
- Allman braces, 4 spaces, `_field` privates, `sealed` by default, always braces.
- Tests: `Spec` suffix, sealed, `Subject_should_behavior()` names, `[Fact(Timeout = 5000)]`.
- Test command shape: `dotnet run --project src/Servus.Akka.Tests/Servus.Akka.Tests.csproj -- -class "Servus.Akka.Tests.Transport.<ClassName>"`.
- Full servus.akka suite: `dotnet run --project src/Servus.Akka.Tests/Servus.Akka.Tests.csproj` — must be green at the end of every task.
- Spec amendment (discovered during planning, supersedes the spec's `IDuplexConnection` sketch): `QuiesceAsync` is **not terminal** — a quiesced connection is reusable by the next lease holder; it returns `bool` (`true` = quiesced cleanly/reusable, `false` = data, EOF, or error surfaced during quiesce → caller must dispose, not pool). `TryEnqueue` returns `false` only after `CompleteAndDrainOutputAsync`/dispose. `ReceiveAsync()` stays parameterless; cancellation is owned by the connection and triggered via `QuiesceAsync`.

---

## Phase 0 — Merge blockers (independent, land first)

### Task 1: Clear `_readInProgress` only inside the generation check

**Files:**
- Modify: `src/Servus.Akka/Transport/Tcp/Client/TcpConnectionStateMachine.cs:45-66`
- Test: `src/Servus.Akka.Tests/Transport/Tcp/Client/TcpConnectionStateMachineSpec.cs`

**Interfaces:**
- Consumes: existing `ITcpTransportEvent` records (`ReadCompleted(TransportBuffer? Buffer, int Gen)`, `ReadFailed(Exception Error, int Gen)`).
- Produces: no signature changes; behavior fix only.

- [ ] **Step 1: Write the failing test** — in the existing spec class (follow its established fixture pattern for constructing the SM; read the file first). The scenario: a current-gen read is in flight, a stale-gen event arrives, and a subsequent pull must NOT issue a second receive.

```csharp
[Fact(Timeout = 5000)]
public void Dispatch_stale_ReadCompleted_should_not_clear_read_in_progress()
{
    // Arrange: acquire a lease (gen N), call RequestRead() so a read is in flight
    // (use a fake connection whose ReceiveAsync returns a non-completed ValueTask),
    // then dispatch a ReadCompleted with Gen = N - 1 and a rented buffer.
    // Act: call RequestRead() again (simulating the next downstream pull).
    // Assert: the fake connection's ReceiveAsync call count is still 1,
    // and the stale event's buffer was disposed (Capacity == 0).
}
```

- [ ] **Step 2: Run it, verify it fails** — expected: call count is 2 (the bug).
- [ ] **Step 3: Fix** — in `Dispatch`, move the reset into the gen branch for both cases:

```csharp
case ReadCompleted e:
    if (e.Gen == _connectionGen)
    {
        _readInProgress = false;
        OnReadCompleted(e.Buffer);
    }
    else
    {
        // Stale read from a torn-down/reconnected lease: the buffer is OWNED by this
        // event (rent-and-receive) — dispose or it leaks the pooled array. A stale
        // event says nothing about the CURRENT gen's read, so _readInProgress is
        // deliberately left alone.
        e.Buffer?.Dispose();
    }

    break;
case ReadFailed e:
    if (e.Gen == _connectionGen)
    {
        _readInProgress = false;
        OnReadFailed(e.Error);
    }

    break;
```

- [ ] **Step 4: Run the test class, then the full suite.** Expected: PASS.
- [ ] **Step 5: Commit** — `fix(tcp): clear _readInProgress only for current-generation read events`

### Task 2: Enforce `ReceiveAsync` non-reentrancy

**Files:**
- Modify: `src/Servus.Akka/Transport/SocketPipeConnection.cs:239-277`
- Test: `src/Servus.Akka.Tests/Transport/SocketPipeConnectionSpec.cs`

**Interfaces:**
- Produces: `ReceiveAsync` throws `InvalidOperationException` on concurrent entry. (This guard is carried forward into `RawSocketConnection`/`StreamConnection` in Phase 2.)

- [ ] **Step 1: Write the failing test** — connected socket pair (follow the existing spec's socket-pair helper), call `ReceiveAsync()` without sending data (read parks), call `ReceiveAsync()` again → assert `InvalidOperationException` synchronously (`Assert.Throws` around the second call; a faulted ValueTask also acceptable via `await Assert.ThrowsAsync`).
- [ ] **Step 2: Run it, verify it fails** — expected: no exception (silent corruption today).
- [ ] **Step 3: Implement** — add a field and guard (plain `int` + `Interlocked` — this is a true cross-thread boundary: completion may release on an IO thread):

```csharp
private int _receiveActive;
```

At the top of `ReceiveAsync` (before the inert-connection branch):

```csharp
if (Interlocked.Exchange(ref _receiveActive, 1) == 1)
{
    throw new InvalidOperationException(
        "Concurrent ReceiveAsync — the connection supports one outstanding receive.");
}
```

Release the flag on every exit path (EOF return, data return, catch-rethrow, and the inert branch). Wrap the existing body in `try { ... } finally { Volatile.Write(ref _receiveActive, 0); }` — a `finally` is the simplest way to cover all five exits. (`Volatile.Write` pairs with the `Interlocked.Exchange` acquire; do not use the `volatile` keyword.)
- [ ] **Step 4: Run test class + full suite.** Expected: PASS.
- [ ] **Step 5: Commit** — `fix(transport): enforce ReceiveAsync single-outstanding contract with a guard`

---

## Phase 1 — WireBuffer

### Task 3: Create `WireBuffer` (merged buffer owner)

**Files:**
- Create: `src/Servus.Akka/Transport/WireBuffer.cs`
- Test: `src/Servus.Akka.Tests/Transport/WireBufferSpec.cs`

**Interfaces:**
- Produces (every later task consumes this exact surface):

```csharp
public sealed class WireBuffer : IMemoryOwner<byte>
{
    public static readonly ArrayPool<byte> SharedPool; // moved from PooledArrayMemoryOwner, same sizing
    public static void ConfigureWrapperPool(int size);  // startup-only, replaces the no-op ConfigurePoolSize
    public int Length { get; set; }
    public int Offset { get; }
    public int Capacity { get; }
    public Memory<byte> Memory { get; }        // Slice(Offset, Length)
    public ReadOnlySpan<byte> Span { get; }
    public Memory<byte> FullMemory { get; }
    public static WireBuffer Rent(int minimumSize);                                  // from SharedPool
    public static WireBuffer Wrap(byte[] array, int offset, int length, ArrayPool<byte>? returnPool = null);
    public static WireBuffer Wrap(IMemoryOwner<byte> owner, int offset, int length); // migration bridge for external owners
    public void Dispose();                     // idempotent (Interlocked guard), returns array then wrapper
}
```

- [ ] **Step 1: Write the failing tests** (port the still-relevant cases from `TransportBufferSpec.cs` + `PooledArrayMemoryOwnerSpec` if present — read both first):

```csharp
[Fact(Timeout = 5000)]
public void Rent_should_provide_writable_memory_of_at_least_requested_size() { /* Rent(4 * 1024), assert Capacity >= 4 * 1024, write/read roundtrip via FullMemory */ }

[Fact(Timeout = 5000)]
public void Wrap_array_with_offset_should_expose_sliced_memory() { /* Wrap(arr, 5, 10): Memory.Length == 10, Span[0] == arr[5], Offset == 5 */ }

[Fact(Timeout = 5000)]
public void Dispose_should_be_idempotent_and_not_double_return_wrapper() { /* Dispose twice; then Rent twice and assert the two rented instances are not ReferenceEquals to each other */ }

[Fact(Timeout = 5000)]
public void Wrap_owner_should_dispose_owner_on_dispose() { /* fake IMemoryOwner records Dispose; Wrap + Dispose → owner.Disposed true */ }

[Fact(Timeout = 5000)]
public void Rent_after_dispose_should_reuse_wrapper_with_reset_state() { /* Rent, set Length, Dispose, Rent again: Length == 0, Offset == 0 */ }
```

- [ ] **Step 2: Run, verify all fail** (type does not exist).
- [ ] **Step 3: Implement:**

```csharp
using System.Buffers;
using static Servus.Senf;

namespace Servus.Akka.Transport;

/// <summary>
/// The transport's single pooled buffer owner: an array from the cross-thread <see cref="SharedPool"/>
/// (or a wrapped external array/owner) plus offset/length, in one pooled wrapper. Ownership transfers
/// with the instance — whoever holds it disposes it exactly once; Dispose returns the array to its
/// pool and the wrapper to the wrapper pool.
/// </summary>
public sealed class WireBuffer : IMemoryOwner<byte>
{
    /// <summary>
    /// Process-wide cross-thread buffer pool (locked per-bucket stacks, no per-core affinity), so a
    /// buffer rented on one thread and returned on another is reused instead of missing the pool.
    /// Per-core ArrayPool&lt;byte&gt;.Shared / MemoryPool.Shared miss on that hop and exhaust under
    /// HTTP/2-3 multiplexing.
    /// </summary>
    public static readonly ArrayPool<byte> SharedPool =
        ArrayPool<byte>.Create(maxArrayLength: 1024 * 1024, maxArraysPerBucket: 1024);

    private static ObjectPool<WireBuffer> _wrapperPool = new(1024);

    /// <summary>Startup-only: replaces the wrapper pool. Not safe once buffers are in flight.</summary>
    public static void ConfigureWrapperPool(int size) => _wrapperPool = new ObjectPool<WireBuffer>(size);

    private byte[]? _array;
    private ArrayPool<byte>? _returnPool;      // null: array is not pool-owned (external Wrap)
    private IDisposable? _externalOwner;       // set only by Wrap(IMemoryOwner, ...)
    private int _offset;

    public int Length { get; set; }
    public int Offset => _offset;
    public int Capacity => _array?.Length ?? 0;
    public Memory<byte> Memory => _array.AsMemory(_offset, Length);
    public ReadOnlySpan<byte> Span => _array.AsSpan(_offset, Length);
    public Memory<byte> FullMemory => _array.AsMemory();

    public static WireBuffer Rent(int minimumSize)
    {
        var buf = RentWrapper();
        buf._array = SharedPool.Rent(minimumSize);
        buf._returnPool = SharedPool;
        return buf;
    }

    public static WireBuffer Wrap(byte[] array, int offset, int length, ArrayPool<byte>? returnPool = null)
    {
        var buf = RentWrapper();
        buf._array = array;
        buf._returnPool = returnPool;
        buf._offset = offset;
        buf.Length = length;
        return buf;
    }

    // Migration bridge: wraps an external IMemoryOwner whose memory is array-backed. The buffer
    // owns 'owner' and disposes it on Dispose. Transport hot paths use the array overloads; this
    // exists for consumers (GaudiHTTP) whose data already lives in a foreign owner.
    public static WireBuffer Wrap(IMemoryOwner<byte> owner, int offset, int length)
    {
        if (!System.Runtime.InteropServices.MemoryMarshal.TryGetArray<byte>(owner.Memory, out var seg))
        {
            throw new ArgumentException("WireBuffer.Wrap requires an array-backed owner.", nameof(owner));
        }

        var buf = Wrap(seg.Array!, seg.Offset + offset, length);
        buf._externalOwner = owner;
        return buf;
    }

    private static WireBuffer RentWrapper()
    {
        if (!_wrapperPool.TryRent(out var buf))
        {
            buf = new WireBuffer();
        }

        return buf;
    }

    public void Dispose()
    {
        var array = Interlocked.Exchange(ref _array, null);
        if (array is null)
        {
            // Double-dispose: the first Dispose already returned this wrapper to the pool. Returning
            // it AGAIN would hand the same instance to two renters (buffer aliasing / silent
            // cross-connection corruption). Log the culprit and bail.
            Tracing.For("Transport").Warning(this,
                "WireBuffer double-dispose detected — wrapper NOT re-returned to pool. Stack: {0}",
                Environment.StackTrace);
            return;
        }

        _returnPool?.Return(array);
        _returnPool = null;
        _externalOwner?.Dispose();
        _externalOwner = null;
        _offset = 0;
        Length = 0;
        _wrapperPool.Return(this);
    }
}
```

Note: `ObjectPool<T>` already lives in `src/Servus.Akka/Transport/ObjectPool.cs` — reuse it, don't create one.
- [ ] **Step 4: Run the spec class.** Expected: PASS.
- [ ] **Step 5: Commit** — `feat(transport): add WireBuffer — single pooled buffer owner (merges TransportBuffer + PooledArrayMemoryOwner)`

### Task 4: Migrate servus.akka off `TransportBuffer`/`PooledArrayMemoryOwner`

**Files:**
- Modify: every file in `src/Servus.Akka/` referencing `TransportBuffer` or `PooledArrayMemoryOwner` — enumerate with `rg -l 'TransportBuffer|PooledArrayMemoryOwner' src/Servus.Akka` (expect: `ITransportOutbound.cs`, `SocketPipeConnection.cs`, both TCP SMs, `TcpTransportEvent.cs`, `QuicStreamState.cs`, both QUIC SMs, QUIC events, `CrossThreadMemoryPool.cs`).
- Delete: `src/Servus.Akka/Transport/TransportBuffer.cs`, `src/Servus.Akka/Transport/PooledArrayMemoryOwner.cs`
- Test: existing specs updated mechanically (same rename mapping); delete `TransportBufferSpec.cs` cases already ported in Task 3.

**Interfaces:**
- Produces: `TransportData.Buffer`, `MultiplexedData.Buffer`, `ReadCompleted.Buffer` etc. are typed `WireBuffer`. `TransportData.Rent(WireBuffer)`, `MultiplexedData.Rent(WireBuffer, StreamTarget)`.

Mapping (mechanical, apply everywhere including tests):

| Old | New |
|---|---|
| `TransportBuffer.Rent(n)` | `WireBuffer.Rent(n)` |
| `TransportBuffer.Wrap(owner, len)` / `(owner, off, len)` | `WireBuffer.Wrap(owner, 0, len)` / `(owner, off, len)` |
| implicit `byte[] → TransportBuffer` | **deleted** — call sites (tests only) use `WireBufferTestExtensions.ToWireBuffer(byte[])` helper: `Rent` + copy + set `Length` (add to test project once) |
| `PooledArrayMemoryOwner.Create(n)` | `WireBuffer.Rent(n)` where the owner immediately became a TransportBuffer; where a raw owner was used for a transient copy (`SocketPipeConnection` send loops), `WireBuffer.Rent(n)` + `Dispose()` |
| `PooledArrayMemoryOwner.SharedPool` | `WireBuffer.SharedPool` |
| `TransportBuffer.ConfigurePoolSize` / `MaxPoolSize` | **deleted** (no-op knob); `WireBuffer.ConfigureWrapperPool` is the real one |

Also update `CrossThreadMemoryPool` if it delegates to `PooledArrayMemoryOwner` (read it first; it may rent owners for pipe segments — those pipe uses disappear in later phases, so for now point it at `WireBuffer.SharedPool`/`WireBuffer.Rent`).

- [ ] **Step 1:** Run the `rg` enumeration, apply the mapping file-by-file. Type/name changes only — no behavior changes in this task.
- [ ] **Step 2:** Build: `dotnet build src/Servus.Akka/Servus.Akka.csproj` — zero errors, zero new warnings.
- [ ] **Step 3:** Full suite. Expected: PASS (same behavior).
- [ ] **Step 4: Commit** — `refactor(transport): migrate to WireBuffer, delete TransportBuffer + PooledArrayMemoryOwner`

Note: GaudiHTTP will not compile against this servus.akka commit — expected; the submodule pointer only moves in Task 14 after the lockstep migration.

---

## Phase 2 — Connection types

### Task 5: `IDuplexConnection` + `RawSocketConnection`

**Files:**
- Create: `src/Servus.Akka/Transport/IDuplexConnection.cs`
- Create: `src/Servus.Akka/Transport/RawSocketConnection.cs`
- Modify: `src/Servus.Akka/Transport/SocketAwaitable.cs` (add a `WireBuffer`-list vectored send)
- Test: `src/Servus.Akka.Tests/Transport/RawSocketConnectionSpec.cs`

**Interfaces:**
- Produces:

```csharp
internal interface IDuplexConnection : IAsyncDisposable
{
    ValueTask<WireBuffer?> ReceiveAsync();          // null = EOF; owned buffer on data; throws on error
    bool TryEnqueue(WireBuffer buffer);             // ownership transfers; false only after output completed/disposed (buffer NOT consumed then — caller disposes)
    ValueTask<bool> QuiesceAsync();                 // cancel pending receive, await settle; true = clean/reusable
    Task CompleteAndDrainOutputAsync();             // no more enqueues; drain + finish send loop
}
```

`RawSocketConnection` constructor: `RawSocketConnection(Socket socket, TransportConnectionOptions opts, Action<int> onFlushed)` — `onFlushed(totalBytes)` fires on the send-loop thread after each drained batch is fully sent and its buffers disposed; the caller's delegate posts an actor message (created once per connection). `TransportConnectionOptions` is a new small options record replacing `SocketPipeConnectionOptions` for the new types: `{ int ReceiveBufferHint = 64 * 1024; long OutputHighWatermark = 512 * 1024; long OutputLowWatermark = 256 * 1024; }` (watermarks are consumed by the SMs in Phase 3, carried here for construction symmetry).

**Design points (implement exactly):**

- **Inbound:** zero-byte probe always (`WaitForData` semantics are now unconditional): probe via the BCL cancellable API `socket.ReceiveAsync(Memory<byte>.Empty, SocketFlags.None, _receiveCts.Token)` — this is the cancellation point for quiesce and it pins no buffer while idle. After the probe completes, rent `WireBuffer.Rent(_receiveHint)` and do the data receive via the existing `SocketAwaitable.ReceiveAsync` (completes synchronously from the kernel buffer after a successful probe — keeps the SMs' sync fast-path alive). Keep the adaptive hint via the existing `AdaptHint` algorithm (lift it verbatim from `SocketPipeConnection.cs:209-231` into a shared `internal static class AdaptiveHint { public static void Adapt(int bytesRead, ref int hint, ref int shrinkStreak); }` in a new file `src/Servus.Akka/Transport/AdaptiveHint.cs` — Task 10 points QUIC at the same helper). Carry over the Task 2 reentrancy guard.
- **Quiesce:** `_receiveCts.Cancel()` + await the in-flight `ReceiveAsync`'s completion (track it in a `Task _pendingReceiveSettled` set from within `ReceiveAsync` — a small `TaskCompletionSource` completed in its `finally`). Outcomes: probe cancelled cleanly → replace `_receiveCts` with a fresh CTS, return `true`; the receive surfaced data/EOF/error during the race → dispose the buffer if any, return `false`. Reusable after `true`: the next `ReceiveAsync` must work.
- **Outbound:** `Channel.CreateUnbounded<WireBuffer>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = true, AllowSynchronousContinuations = false })`. `TryEnqueue` = `_channel.Writer.TryWrite` (false after complete → caller keeps ownership). Send loop (`Task.Run`, token NOT passed to `Task.Run` itself — same rationale comment as `SocketPipeConnection.cs:171-174`): `await reader.WaitToReadAsync(ct)`, then `TryRead` into a reusable `List<WireBuffer>` (drain everything available, cap 64 per batch), single buffer → `SocketAwaitable.SendAsync(socket, buffer.Memory)`, multiple → new `SocketAwaitable.SendManyAsync(socket, IReadOnlyList<WireBuffer> buffers)` that fills the internal `_bufferList` with `new ArraySegment<byte>(FullMemory-array, Offset, Length)` per buffer (mirror `SetBufferList`, `MemoryMarshal.TryGetArray` on `buffer.Memory`). **Handle partial sends**: loop until the batch's total bytes are transferred, re-issuing from the offset (`BytesTransferred` accounting); the old pipe loop assumed full transfer — do not carry that assumption over. After full send: dispose each buffer, `onFlushed(totalBytes)`, clear the list. Teardown exceptions classified by the existing `IsTeardownException` predicate (move it to the interface file as `internal static class ConnectionErrors`); on teardown, drain remaining channel items and dispose them.
- **Dispose:** cancel CTS, `Shutdown(Both)` + `Close` guarded as today (`SocketPipeConnection.cs:444-456`), complete channel writer, await send loop, dispose any drained buffers.

- [ ] **Step 1: Write failing tests** (socket-pair pattern from `SocketPipeConnectionSpec.cs` — read it first and reuse its helpers):

```csharp
[Fact(Timeout = 5000)] public void ReceiveAsync_should_deliver_sent_bytes_in_owned_buffer() { }
[Fact(Timeout = 5000)] public void ReceiveAsync_should_return_null_on_remote_close() { }
[Fact(Timeout = 5000)] public void ReceiveAsync_should_throw_on_concurrent_call() { }
[Fact(Timeout = 5000)] public void TryEnqueue_should_send_buffer_and_invoke_onFlushed_with_byte_count() { }
[Fact(Timeout = 5000)] public void TryEnqueue_many_should_coalesce_into_vectored_send_and_deliver_all_bytes() { /* enqueue 10 x 1024-byte buffers before loop wakes; peer receives 10240 correct bytes; onFlushed total == 10240 */ }
[Fact(Timeout = 5000)] public void QuiesceAsync_should_cancel_idle_probe_and_return_true() { }
[Fact(Timeout = 5000)] public void ReceiveAsync_after_clean_quiesce_should_work_again() { }
[Fact(Timeout = 5000)] public void QuiesceAsync_should_return_false_when_data_races_in() { /* send bytes just before quiesce; poll until false; assert no buffer leak via wrapper-pool roundtrip */ }
[Fact(Timeout = 5000)] public void DisposeAsync_with_pending_receive_should_not_hang() { }
[Fact(Timeout = 5000)] public void TryEnqueue_after_CompleteAndDrainOutput_should_return_false_and_leave_ownership() { }
```

- [ ] **Step 2: Run, verify failures** (types missing).
- [ ] **Step 3: Implement** per the design points. Keep `RawSocketConnection` under ~350 lines; if it grows past that, extract the send loop into `src/Servus.Akka/Transport/SendLoop.cs`.
- [ ] **Step 4: Run spec class + full suite.** Expected: PASS.
- [ ] **Step 5: Commit** — `feat(transport): IDuplexConnection + RawSocketConnection (probe-gated receive, channel outbound, quiesce)`

### Task 6: `StreamConnection`

**Files:**
- Create: `src/Servus.Akka/Transport/StreamConnection.cs`
- Test: `src/Servus.Akka.Tests/Transport/StreamConnectionSpec.cs`

**Interfaces:**
- Consumes: `IDuplexConnection`, `WireBuffer`, `AdaptiveHint`, `ConnectionErrors` from Task 5.
- Produces: `StreamConnection(Stream stream, TransportConnectionOptions opts, Action<int> onFlushed, bool quicAware = false)`.

**Design points:**

- **Inbound:** no zero-byte probe (plain streams — Memory/test streams — return 0 immediately on empty reads; `SslStream`/`QuicStream` would support it, but one uniform rule is worth the pinned buffer, matching current behavior): rent at hint, `await _stream.ReadAsync(buffer.FullMemory, _receiveCts.Token)`, 0 → dispose + null (EOF), adapt hint, return owned buffer. Reentrancy guard identical to Task 5. Quiesce: `Cancel()` + await settle; because a buffer IS rented here, quiesce-during-pending-read that gets cancelled disposes the rent and returns `true`; data completing in the race → `false` + dispose. `Stream.ReadAsync` honors the token on `SslStream`/`NetworkStream`/`QuicStream`.
- **quicAware:** wraps read/write errors: `QuicException` with `QuicError.ConnectionAborted`/`StreamAborted`/graceful codes maps to EOF (`null`) instead of throwing — lift the exact classification currently in the QUIC send/receive handling (`RunQuicSendLoop`'s `WritesClosed` pre-check at `SocketPipeConnection.cs:388-394` and the graceful-close mapping introduced by commit `5585069`; read both QUIC SMs' catch blocks before writing this). Send loop: before each write, if `stream is QuicStream q && q.WritesClosed.IsCompleted` → drain-dispose and finish.
- **Outbound:** same channel structure as Task 5. Per drained batch: if total bytes `<= 4 * 1024 * batchCount` heuristic is over-engineering — use the simple rule: single buffer → `WriteAsync(buffer.Memory)`; multiple small (each `Length < 4 * 1024`) → coalesce all into one `WireBuffer.Rent(total)` copy and one `WriteAsync`; otherwise sequential `WriteAsync` per buffer. One `FlushAsync` per drained batch (not per buffer). Dispose sent buffers, `onFlushed(total)`.

- [ ] **Step 1: Failing tests** — mirror Task 5's list over a duplex stream pair (use `System.IO.Pipelines.Pipe`-backed or `NetworkStream` over the socket pair — reuse the spec helper), plus:

```csharp
[Fact(Timeout = 5000)] public void Send_batch_of_small_buffers_should_produce_single_coalesced_write() { /* custom recording Stream counts WriteAsync calls: 8 x 512-byte enqueues before loop wakes → 1 write, correct bytes */ }
[Fact(Timeout = 5000)] public void Send_large_buffers_should_write_sequentially_without_coalescing() { /* 2 x 64 * 1024 → 2 writes */ }
```

- [ ] **Step 2: Run, verify failures.**
- [ ] **Step 3: Implement.** Share the drain/batch skeleton with Task 5 via `SendLoop.cs` if it was extracted; otherwise keep the two loops parallel but note the duplication for Phase 5.
- [ ] **Step 4: Run spec class + full suite.**
- [ ] **Step 5: Commit** — `feat(transport): StreamConnection (rent-and-receive over Stream, coalescing channel outbound, QUIC-aware mode)`

### Task 7: Lease quiesce in the connection manager

**Files:**
- Modify: `src/Servus.Akka/Transport/Tcp/ConnectionLease.cs` (Connection typed `IDuplexConnection`; drop the `OutputWriter` passthrough — Phase 3 removes its consumers, so this task only compiles once Task 8 lands; **do Tasks 7+8 on one branch, commit together or sequence 8 before 7's build check**)
- Modify: `src/Servus.Akka/Transport/Tcp/Client/TcpConnectionManagerActor.cs:114-143` (`OnRelease`) and the `Release`/new `Quiesced` messages
- Modify: `src/Servus.Akka/Transport/Tcp/Client/TcpConnectionFactory.cs` (construct `RawSocketConnection`/`StreamConnection` instead of `SocketPipeConnection` — read it first; it currently picks socket-direct vs stream by TLS)
- Test: `src/Servus.Akka.Tests/Transport/Tcp/ConnectionLeaseSpec.cs`, `src/Servus.Akka.Tests/Transport/Tcp/Client/TcpConnectionManagerActorSpec.cs` (locate via `rg -l 'TcpConnectionManagerActor' src/Servus.Akka.Tests`)

**Interfaces:**
- Produces: `internal sealed record Quiesced(ConnectionLease Lease, bool Clean)` handled by the manager; reusable `Release` path becomes: quiesce → `Quiesced(clean)` → pool or dispose.

- [ ] **Step 1: Failing tests:**

```csharp
[Fact(Timeout = 5000)] public void Release_with_reuse_should_quiesce_before_pooling() { /* fake IDuplexConnection: QuiesceAsync returns a TCS-controlled ValueTask<bool>; Release; assert an immediate Acquire does NOT get the lease until the TCS completes true */ }
[Fact(Timeout = 5000)] public void Release_with_dirty_quiesce_should_dispose_not_pool() { /* QuiesceAsync → false; assert lease disposed, next Acquire establishes fresh */ }
```

- [ ] **Step 2: Run, verify failures.**
- [ ] **Step 3: Implement** in `OnRelease` — replace the current immediate `Pending`-hand-off/`Idle.Push` (lines 134-142) for the reuse path:

```csharp
if (!msg.CanReuse || !msg.Lease.IsAlive())
{
    host.Leases.Remove(msg.Lease);
    msg.Lease.Dispose();
    ServeNextPending(host);
    return;
}

// Quiesce before the lease becomes acquirable: the releasing consumer may still have a
// receive in flight on this connection. Handing it out with that operation pending is the
// corruption + byte-loss path — the lease only reaches Idle/Pending via Quiesced(clean).
var lease = msg.Lease;
QuiesceLease(lease).PipeTo(Self, success: clean => new Quiesced(lease, clean));

// ...

private static async Task<bool> QuiesceLease(ConnectionLease lease)
{
    try
    {
        return await lease.Connection.QuiesceAsync();
    }
    catch
    {
        return false;
    }
}
```

`Quiesced` handler: `Clean` → the old lines 134-142 (serve `Pending` first, else `Idle.Push`); dirty → `host.Leases.Remove` + `Dispose` + `ServeNextPending`. (Host lookup inside the handler reuses `FindHostKey`.)
- [ ] **Step 4: Run both spec classes + full suite.**
- [ ] **Step 5: Commit** — `fix(pool): quiesce connections before reuse — no lease is pooled with a pending receive` (combined with Task 8 if sequenced together).

---

## Phase 3 — TCP state machines on the new connection

### Task 8: Client SM — channel outbound, watermarks, cached transforms

**Files:**
- Modify: `src/Servus.Akka/Transport/Tcp/Client/TcpConnectionStateMachine.cs`
- Modify: `src/Servus.Akka/Transport/Tcp/TcpTransportEvent.cs` (replace `PipeFlushComplete` with `SendFlushed(int Bytes, int Gen)`)
- Create: `src/Servus.Akka/Transport/Tcp/ReadEventState.cs` (cached transforms, shared with the server SM)
- Test: `src/Servus.Akka.Tests/Transport/Tcp/Client/TcpConnectionStateMachineSpec.cs`

**Interfaces:**
- Consumes: `IDuplexConnection` (Task 5), `ConnectionLease.Connection` (Task 7).
- Produces:

```csharp
// ReadEventState.cs — one instance per (connection, generation); the delegates capture only
// this immutable pair, so PipeTo allocates nothing per read (same model as QuicStreamState's
// cached transforms, see QuicStreamState.cs:36-52).
internal sealed class ReadEventState(int gen)
{
    public readonly Func<WireBuffer?, ITcpTransportEvent> ReadSuccess = buffer => new ReadCompleted(buffer, gen);
    public readonly Func<Exception, ITcpTransportEvent> ReadFailure = ex => new ReadFailed(ex, gen);
}
```

SM changes:

1. Delete `_pendingWrites`, `_needsFlush`, `_flushInProgress`, `WriteToOutputPipe`, `FlushIfNeeded`, `FlushPendingWrites`, `PipeFlushComplete` handling. Replace with `long _bytesInFlight` and the watermark rule; keep a pre-lease `Queue<WireBuffer> _preConnectWrites` ONLY for buffers pushed before `LeaseAcquired` (the old `_pendingWrites` served both roles — the pre-connect role remains, drained into `TryEnqueue` on acquisition; orphan-dispose on `PostStop`/reconnect exactly as today at lines 134-137 / 299-302).
2. `HandleTransportData`: extract buffer, `data.Return()` (**the missing return — this fixes the client-side wrapper starvation**), `TryEnqueue`; on `false` → dispose buffer + `OnInboundComplete(DisconnectReason.Error)`. `_bytesInFlight += buffer.Length`; pull upstream (`ops.OnSignalPullOutbound()`) only while `_bytesInFlight < HighWatermark` (from `TransportConnectionOptions`, plumbed via lease/factory).
3. New `SendFlushed` case in `Dispatch`: gen-checked; `_bytesInFlight -= e.Bytes`; if it just crossed below LowWatermark → `ops.OnSignalPullOutbound()`; if `_upstreamFinished && _bytesInFlight == 0` → run the `HandleUpstreamFinish` completion block (this fixes the current flush-in-flight shutdown hang at lines 94-109: today nothing re-checks).
4. The `onFlushed` delegate given to the connection at lease acquisition: `bytes => self.Tell(new SendFlushed(bytes, gen))` — created once per lease, allocation-free per batch. (The connection is built by the factory before the SM exists; pass the delegate through `AcquireAsync`? No — keep it simple: the connection's `onFlushed` is a settable property `Action<int>? OnFlushed` assigned by the SM in `OnLeaseAcquired` before the first enqueue. Amend `IDuplexConnection` accordingly: `Action<int>? OnFlushed { get; set; }`, invoked by the send loop when non-null. Update Tasks 5/6 tests to set it before enqueueing.)
5. `RequestRead`: keep the sync-budget loop; async path becomes `readTask.PipeTo(self, success: _readState.ReadSuccess, failure: _readState.ReadFailure)` where `_readState = new ReadEventState(_connectionGen)` is refreshed in `OnLeaseAcquired`/`CleanupTransport` (once per gen — the closure allocs move from per-read to per-connection).
6. `HandleUpstreamFinish`: quiesce is NOT called here — release-side quiesce lives in the manager (Task 7). Condition becomes `_preConnectWrites.Count == 0 && _bytesInFlight == 0`.

- [ ] **Step 1: Failing tests** (rewrite the flush-oriented cases in the spec; read the whole spec first):

```csharp
[Fact(Timeout = 5000)] public void HandleTransportData_should_enqueue_buffer_and_return_wrapper() { /* fake connection records TryEnqueue; TransportData wrapper observably returned: TransportData.Rent after the call reuses the same instance */ }
[Fact(Timeout = 5000)] public void HandlePush_above_high_watermark_should_not_pull_upstream() { }
[Fact(Timeout = 5000)] public void SendFlushed_below_low_watermark_should_resume_pull() { }
[Fact(Timeout = 5000)] public void UpstreamFinish_with_bytes_in_flight_should_complete_after_final_SendFlushed() { }
[Fact(Timeout = 5000)] public void RequestRead_async_path_should_reuse_cached_transforms() { /* two async reads in same gen: PipeTo transform delegates are ReferenceEquals across reads (expose ReadEventState internal for test) */ }
```

- [ ] **Step 2: Run, verify failures.**
- [ ] **Step 3: Implement** items 1–6.
- [ ] **Step 4: Run spec class + full suite.**
- [ ] **Step 5: Commit** — `feat(tcp): client SM on channel outbound — watermark backpressure, cached read transforms, wrapper return fix`

### Task 9: Server SM — same discipline

**Files:**
- Modify: `src/Servus.Akka/Transport/Tcp/Listener/TcpServerStateMachine.cs`
- Test: `src/Servus.Akka.Tests/Transport/Tcp/Listener/TcpServerStateMachineSpec.cs`

**Interfaces:**
- Consumes: `IDuplexConnection`, `ReadEventState`, `SendFlushed` (Task 8).

Changes mirror Task 8 with the server's simpler lifecycle (no lease/reconnect): `Start()` constructs `RawSocketConnection` (plaintext, `socket is not null && sslStream is null`) or `StreamConnection` (TLS) with `TransportConnectionOptions`; `HandleTransportData` (lines 138-167) becomes extract → `data.Return()` → `TryEnqueue` → watermark accounting (replacing the per-item `FlushAsync` + `PipeFlushComplete` round-trip — **this removes the server's per-write flush stall and unifies the two outbound disciplines**); `SendFlushed` drives `ops.OnSignalPullOutbound()` on the low-watermark crossing; add the client's `_readInProgress` guard (the server currently relies on the stage's `_readRequested` only — give the SM the same self-defense, cleared only inside the gen check); async reads via a per-gen `ReadEventState`; `Cleanup()` quiesces before dispose (`_connection.QuiesceAsync()` fire-and-forget is NOT acceptable — `Cleanup` is sync; instead rely on `DisposeAsync` which cancels the receive CTS internally, and dispose-drains the channel; the PostStop dead-letter leak is closed because a cancelled receive disposes its own rent inside the connection).

- [ ] **Step 1: Failing tests** — server-side mirrors of Task 8's five tests plus `Dispatch_stale_ReadCompleted_should_not_clear_read_in_progress` (the new guard).
- [ ] **Step 2: Run, verify failures.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run spec class + `TcpTransportEventSpec` + full suite.**
- [ ] **Step 5: Commit** — `feat(tcp): server SM on channel outbound — unified with client discipline`

---

## Phase 4 — QUIC

**Read first, before any Phase 4 edit:** `src/Servus.Akka/Transport/Quic/Client/QuicTransportStateMachine.cs`, `src/Servus.Akka/Transport/Quic/Listener/QuicServerStateMachine.cs` (locate with `rg --files src/Servus.Akka/Transport/Quic`), `QuicStreamState.cs`, `QuicConnectionStage.cs`, `QuicListenerStage.cs`, `Quic/Client/QuicTransportFactory.cs`, and the QUIC event types. These two SMs are the largest files in the transport; the audit anchors below are from HEAD `9b511dc` and may drift a few lines.

### Task 10: `QuicStreamState` on `StreamConnection`; delete the pipe read path

**Files:**
- Modify: `src/Servus.Akka/Transport/Quic/QuicStreamState.cs`
- Modify: both QUIC SMs (all `PipeStreamReadResult`/`PipeStreamReadFailed`/`InputReader`/`PendingAdvance` handling; client `QuicTransportStateMachine.cs:303-466`, server `QuicServerStateMachine.cs:214-357` regions)
- Delete: `SocketPipeConnection.CreateForQuic`, `CreateWithStreamReader` usages from `QuicStreamState.AttachConnection` (`QuicStreamState.cs:214-227`)
- Test: `src/Servus.Akka.Tests/Transport/Quic/QuicStreamStateSpec.cs` + affected SM specs

**Interfaces:**
- Consumes: `StreamConnection` with `quicAware: true` (Task 6).
- Produces: `QuicStreamState.AttachConnection(Stream stream, long rawStreamId = 0)` constructs `new StreamConnection(stream, opts, onFlushed, quicAware: true)` for BOTH QuicStream and plain-stream attach — one path. New read API on the state: `ValueTask<WireBuffer?> ReceiveAsync()` delegating to the connection; a per-state cached transform pair replacing `DirectReadTransform`/`PipeReadTransform`/`FailureReadTransform` (keep the `capture-only-this` model documented at `QuicStreamState.cs:36-45`): `Func<WireBuffer?, IQuicTransportEvent> ReadSuccess` → `StreamReadCompleted(this, buffer)`, `Func<Exception, IQuicTransportEvent> ReadFailure` → `StreamReadFailed(this, ex)` (new event records; delete `DirectStreamReadComplete`, `PipeStreamReadResult`, `PipeStreamReadFailed`).

Consequences to implement:

1. `PendingReadBuffer`/`BeginDirectRead`/`ReadInFlight`/`_tornDownWithReadInFlight`/`CompleteRead` and the never-repool teardown rule (`QuicStreamState.cs:72-111, 178-196`) are **deleted** — buffer ownership during an in-flight read lives inside `StreamConnection`, and `DisposeAndReturnAsync` first `await _connection.QuiesceAsync()` then always repools. The stale-completion hazard the never-repool rule defended against is gone because quiesce awaits the read's settlement before the state is reset.
2. Outbound: `Write(WireBuffer)` → `_connection.TryEnqueue` (opening-buffer queue for pre-attach writes stays, drained via `TryEnqueue` in `AttachConnection`); `WriteToOutputPipe`'s copy (`QuicStreamState.cs:307-314`) is deleted; `FlushWrites()` is deleted (the channel send loop flushes per batch — `HandleCompleteWrites`'s discarded `ValueTask` at the SM call sites goes away with it); `CompleteWritesInternal` uses `_connection.CompleteAndDrainOutputAsync()` then `qs.CompleteWrites()` as today.
3. Read-completion handling: extract ONE shared handler used by both SMs — `internal static class QuicStreamReads` with a method that takes the SM's ops facade, the state, and the completed buffer, and performs: null → `state.OnReadCompleted()` phase transition + inbound EOF signal; data → `MultiplexedData.Rent(buffer, streamId)` push + `AdaptiveHint` (replace `AdaptReadHint`/`ResetReadHint` at `QuicStreamState.cs:113-142` with the shared helper from Task 5, initial hint from `TransportConnectionOptions.ReceiveBufferHint` — this also wires `ReceiveBufferHint` into QUIC, closing the hardcoded 4 KB at `QuicStreamState.cs:64`). Both SMs' duplicated regions collapse to calls into it.

- [ ] **Step 1: Failing tests** — port the existing `QuicStreamStateSpec` read-pump cases to the new API; add `DisposeAndReturn_with_read_in_flight_should_quiesce_and_repool` (rent two states, dispose-with-pending-read, assert the instance IS reused by the next `Rent` — the inverse of today's never-repool assertion).
- [ ] **Step 2: Run, verify failures.**
- [ ] **Step 3: Implement 1–3.** Build both SMs clean.
- [ ] **Step 4: Run all QUIC spec classes + full suite.**
- [ ] **Step 5: Commit** — `refactor(quic): streams on StreamConnection — one rent-and-receive read path, pipe fallback deleted`

### Task 11: Pull-gate QUIC inbound

**Files:**
- Modify: `src/Servus.Akka/Transport/Quic/Client/QuicConnectionStage.cs` (`onPull` region, currently drains `_pendingReads` only, `:50-57`), the client SM's `RequestStreamRead` re-arm sites (`QuicTransportStateMachine.cs:375, 411` region), and the server equivalents
- Test: SM specs + a new `src/Servus.Akka.Tests/Transport/Quic/QuicInboundBackpressureSpec.cs`

**Discipline (mirror TCP's, adapted for multiplexing):** per stream, at most ONE in-flight read AND at most ONE undelivered completed item; a stream's read is re-armed only when its item is pushed downstream (or on stream open). `_pendingReads` is therefore bounded by the live-stream count. Implementation: per-`QuicStreamState` `bool ReadArmed`; on completion-delivered-to-stage, if the item was pushed immediately → re-arm; if it queued → re-arm at the dequeue site in `onPull`. Keep the sync-read budget (8) per stream on both client AND server (the server currently lacks it — `QuicServerStateMachine.cs:250-253` region).

- [ ] **Step 1: Failing test:**

```csharp
[Fact(Timeout = 5000)] public void Slow_consumer_should_bound_pending_reads_to_stream_count() { /* 4 streams, flood each with N chunks, never pull downstream: assert at most 4 items queued and at most 4 receives issued beyond the delivered ones */ }
```

- [ ] **Step 2: Run, verify it fails** (unbounded growth today).
- [ ] **Step 3: Implement** on client and server.
- [ ] **Step 4: Run QUIC specs + full suite.**
- [ ] **Step 5: Commit** — `feat(quic): pull-gated inbound — pending reads bounded by live stream count`

### Task 12: QUIC lifecycle fixes + conflate removal

**Files:**
- Modify: `QuicServerStateMachine.cs` accept loop (`:405-415` region), `QuicListenerStage.cs` handle (`:220-235` region), `QuicTransportStateMachine.HandleUpstreamFinish` (`:90-101`), `Quic/Client/QuicTransportFactory.cs:13-19`, `Quic/Client/QuicConnectionManagerActor.cs:213-215` (eviction lifetime)
- Test: affected spec classes; add cases listed below.

Four independent fixes, one commit each if preferred, or one combined:

1. **Accept-loop terminal null** (server): adopt the client's semantics (`QuicTransportStateMachine.cs:607-615` has the rationale comment) — null from accept is terminal, stop the loop. `QuicConnectionHandle` in `QuicListenerStage` stops swallowing ALL exceptions into null: catch only the transient set (`QuicException` with transient codes — mirror the client's classification), rethrow/propagate terminal ones. Test: dead connection → accept loop exits, no spin (assert accept call count stops growing).
2. **`HandleUpstreamFinish` teardown** (client): dispose `_streams` states and return the connection lease exactly as `CleanupTransport` does (read both methods, extract the shared teardown into a private method). Test: upstream finish with open streams → all stream states disposed, lease released.
3. **Conflate removal**: delete the `ConflateWithSeed(item => new List<ITransportOutbound> { item }, ...)` batching in `QuicTransportFactory` — with per-stream channel outbound (Task 10) the batching lives in the send loops; the stage takes single items like the server. Test: existing client SM specs stay green (behavioral no-op; the win is the deleted per-cycle `List`).
4. **Eviction honors config**: `QuicConnectionManagerActor` eviction uses `QuicTransportOptions.ConnectionLifetime` instead of the hardcoded `TimeSpan.FromMinutes(10)`; also replace the per-tick LINQ `Where/ToList` with the TCP manager's pop/push pattern (`TcpConnectionManagerActor.cs:183-213`). Test: lifetime = 50 ms fake-clock eviction evicts; infinite lifetime never evicts.
5. **Migration-check dedup**: the connection-migration timer + `CheckForConnectionMigration` logic is duplicated verbatim across both QUIC SMs — extract into one shared internal helper (same move as `AdaptiveHint`). Behavioral no-op; existing specs stay green.

- [ ] **Steps:** for each fix: failing test → verify fail → implement → verify pass. Then full suite.
- [ ] **Commit(s)** — `fix(quic): terminal accept-loop null + upstream-finish teardown; perf(quic): drop conflate list, honor ConnectionLifetime`

---

## Phase 5 — Deletion & config cleanup

### Task 13: Delete `SocketPipeConnection` and dead options

**Files:**
- Delete: `src/Servus.Akka/Transport/SocketPipeConnection.cs`, `src/Servus.Akka/Transport/SocketPipeConnectionOptions.cs`; `IOQueue` if `rg IOQueue src/Servus.Akka` shows no remaining consumer; `CrossThreadMemoryPool` likewise (its pipe clients are gone; keep only if something still rents from it).
- Modify: `src/Servus.Akka/Transport/TransportOptions.cs` (delete `InputPauseThreshold`, `InputResumeThreshold`, `WaitForData`; keep `OutputPauseThreshold`/`OutputResumeThreshold` — they now feed `TransportConnectionOptions.OutputHighWatermark`/`LowWatermark`; keep `ReceiveBufferHint`, `MinimumSegmentSize` only if still consumed, else delete), `src/Servus.Akka/Transport/ListenerOptions.cs` (same deletions), `TcpPoolConfig.cs` (**decide `IdleTimeout`**: wire it — evict idle leases older than `IdleTimeout` in `OnEvict` using a `ConnectionLease` idle-since timestamp set on `Idle.Push` — or delete the property; wiring is one field + one check, prefer wiring), `ConnectionLease.cs` (drop `OutputWriter` if not already gone).
- Modify: `src/Servus.Akka.Tests/Transport/ListenerOptionsSpec.cs:34-35` and any spec asserting deleted defaults.
- Test: build + full suite; `rg 'InputPause|InputResume|WaitForData|SocketPipeConnection|PipeFlushComplete|CreateInert'` over `src/` must return nothing.

- [ ] **Step 1:** Delete/modify per list; fix compile fallout (should be none if Phases 2–4 were complete — any fallout is a missed migration, fix it there, not with a shim).
- [ ] **Step 2:** `IdleTimeout` wiring + test (`Idle lease older than IdleTimeout should be evicted on tick`).
- [ ] **Step 3:** Full suite green; the `rg` sweep above returns empty.
- [ ] **Step 4: Commit** — `refactor(transport): delete SocketPipeConnection and dead pipe-era configuration`

---

## Phase 6 — GaudiHTTP lockstep + validation gate

### Task 14: GaudiHTTP migration

**Files (GaudiHTTP repo, `D:\GIT\Akka.Streams.Http\src`):**
- Modify: every consumer of `TransportBuffer`/`PooledArrayMemoryOwner` — enumerate with `rg -l 'TransportBuffer|PooledArrayMemoryOwner' GaudiHTTP GaudiHTTP.Tests` (memory notes say the H2 frame emitters use `TransportBuffer.Wrap(owner, offset, length)` for header-headroom zero-copy and body buffers use `PooledArrayMemoryOwner`/`CrossThreadBufferPool` — the Task 3 `Wrap(byte[]/IMemoryOwner, offset, length)` overloads cover both).
- Modify: submodule pointer `lib/servus.akka` → the Phase 5 commit.

Apply the Task 4 mapping table verbatim. Where GaudiHTTP created `PooledArrayMemoryOwner.Create(n)` + `TransportBuffer.Wrap(owner, off, len)` as a pair, collapse to `WireBuffer.Rent(n)` + write at offset + set `Length`/use `Wrap(array-of-rented? , ...)` — prefer renting the `WireBuffer` up front and writing into `FullMemory` with headroom, then `buffer.Length = ...` is NOT enough for offset — for the headroom pattern use `WireBuffer.Wrap(WireBuffer.SharedPool-rented byte[] ...)`? **No:** the clean form is a new `WireBuffer.RentWithOffset(int minimumSize, int offset, int length)`? — **Stop: do not invent API in this task.** If the headroom call sites don't collapse cleanly onto `Rent` + `Wrap(byte[], offset, length, SharedPool)` (rent the array from `WireBuffer.SharedPool` directly, wrap with offset), flag it and add the missing overload back in servus.akka with a test, as its own commit.

- [ ] **Step 1:** Bump submodule, run the mapping, build `GaudiHTTP.slnx` clean (zero diagnostics — run Roslyn Navigator `get_diagnostics` per CLAUDE.md).
- [ ] **Step 2:** Unit+stage suite: `dotnet run --project GaudiHTTP.Tests/GaudiHTTP.Tests.csproj` (from `src/`) — green (~5930 tests).
- [ ] **Step 3:** All three integration suites (Client, End2End, Server) — green. `$env:GAUDIHTTP_TEST_BACKEND = "kestrel"` if Docker is unavailable.
- [ ] **Step 4: Commit** (GaudiHTTP repo) — `deps(submodule): bump servus.akka to unified rent-and-receive transport; migrate to WireBuffer`

### Task 15: Benchmark gate

**Files:** none (measurement only; results land in `src/BenchmarkDotNet.Artifacts/{timestamp}/`).

From `D:\GIT\Akka.Streams.Http\src`, `-c Release`:

- [ ] **Step 1: Server upload** (THE regression axis — rent-and-receive gave up receive/processing overlap): `dotnet run -c Release --project GaudiHTTP.Benchmarks/GaudiHTTP.Benchmarks.csproj -- --filter '*Upload*'` — compare Mean/Req-per-sec against the current-branch baseline (run the same filter on the pre-plan submodule commit first if no recent artifact exists). Acceptance: no worse than −5% vs the branch baseline; note the memory item "server upload 2× slow" — flag any improvement/regression against it.
- [ ] **Step 2: H3 + client upload allocation** (expected wins): `--filter '*ClientAllocationBenchmarks*'` and the H3 upload allocation class — read ONLY the EventPipe total from `*.alloc-by-type.json` (never the MemoryDiagnoser column). Acceptance: H3 upload allocation materially down vs baseline (memory item: 83 GB pathological case).
- [ ] **Step 3: Plaintext throughput canary**: `--filter '*GaudiServerPlaintextBenchmark*'` — within noise of baseline.
- [ ] **Step 4:** Charts + summary: `cd docs && npm run charts -- ../src/GaudiHTTP.Benchmarks/BenchmarkDotNet.Artifacts/<run>` ; write the numbers into the PR description / report to the user. **Do not merge past this gate on a regression — report and stop.**

---

## Self-review notes (resolved during planning)

- Spec's `ReceiveAsync(CancellationToken)` → parameterless + connection-owned CTS; spec's terminal `TryEnqueue=false on quiesce` → quiesce is reusable, false only after output completion (Global Constraints amendment).
- `OnFlushed` moved from constructor arg to settable property in Task 8 item 4 — Tasks 5/6 should implement it as the property from the start.
- Task 7/8 have a compile-order coupling (lease type change) — sequence 8's SM edits before 7's final build, or land them as one branch.
- QUIC SM line anchors are advisory (files unread at planning time); each Phase 4 task starts with a read-first instruction.
