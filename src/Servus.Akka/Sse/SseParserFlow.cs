using System.Text;
using Akka;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Stage;

namespace Servus.Akka.Sse;

public static class SseParserFlow
{
    public static Flow<ReadOnlyMemory<byte>, ServerSentEvent, NotUsed> Instance { get; }
        = Flow.FromGraph(new SseParserStage());
}

internal sealed class SseParserStage : GraphStage<FlowShape<ReadOnlyMemory<byte>, ServerSentEvent>>
{
    private readonly Inlet<ReadOnlyMemory<byte>> _in = new("SseParserStage.in");
    private readonly Outlet<ServerSentEvent> _out = new("SseParserStage.out");

    public override FlowShape<ReadOnlyMemory<byte>, ServerSentEvent> Shape => new(_in, _out);

    protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes)
        => new SseParserLogic(this);

    private sealed class SseParserLogic : GraphStageLogic
    {
        private readonly SseParserStage _stage;
        private readonly StringBuilder _lineBuffer = new();
        private readonly StringBuilder _dataAccumulator = new();
        private readonly Queue<ServerSentEvent> _pending = new();

        private string? _eventType;
        private string? _id;
        private TimeSpan? _retry;
        private bool _bomChecked;
        private bool _hasData;
        private bool _upstreamFinished;
        private bool _upstreamWaiting;

        public SseParserLogic(SseParserStage stage) : base(stage.Shape)
        {
            _stage = stage;
            SetHandler(stage._in,
                onPush: () =>
                {
                    _upstreamWaiting = false;
                    var chunk = Grab(stage._in);
                    var bytes = chunk.ToArray();

                    var startIndex = 0;
                    if (!_bomChecked)
                    {
                        _bomChecked = true;
                        if (bytes is [0xEF, 0xBB, 0xBF, ..])
                        {
                            startIndex = 3;
                        }
                    }

                    var text = Encoding.UTF8.GetString(bytes, startIndex, bytes.Length - startIndex);
                    ProcessText(text);
                    DrainPending(stage);
                },
                onUpstreamFinish: () =>
                {
                    _upstreamFinished = true;

                    if (_lineBuffer.Length > 0)
                    {
                        ProcessField(_lineBuffer.ToString());
                        _lineBuffer.Clear();
                    }

                    if (_hasData)
                    {
                        var data = _dataAccumulator.ToString();
                        if (data.Length > 0 && data[^1] == '\n')
                        {
                            data = data[..^1];
                        }

                        var evt = new ServerSentEvent(
                            Data: data,
                            EventType: _eventType ?? "message",
                            Id: _id,
                            Retry: _retry);
                        _pending.Enqueue(evt);
                    }

                    DrainPending(stage);
                });

            SetHandler(stage._out,
                onPull: () =>
                {
                    DrainPending(stage);
                });
        }

        public override void PreStart()
        {
            Pull(_stage._in);
            _upstreamWaiting = true;
        }

        private void DrainPending(SseParserStage stage)
        {
            while (IsAvailable(stage._out) && _pending.Count > 0)
            {
                var evt = _pending.Dequeue();
                Push(stage._out, evt);
            }

            if (!IsAvailable(stage._out))
            {
                return;
            }

            if (_upstreamFinished && _pending.Count == 0)
            {
                CompleteStage();
            }
            else if (!_upstreamWaiting && !_upstreamFinished)
            {
                Pull(stage._in);
                _upstreamWaiting = true;
            }
        }

        private void ProcessText(string text)
        {
            var i = 0;
            while (i < text.Length)
            {
                var lineEnd = -1;
                var endLength = 0;

                for (var j = i; j < text.Length; j++)
                {
                    if (j < text.Length - 1 && text[j] == '\r' && text[j + 1] == '\n')
                    {
                        lineEnd = j;
                        endLength = 2;
                        break;
                    }

                    if (text[j] == '\r' || text[j] == '\n')
                    {
                        lineEnd = j;
                        endLength = 1;
                        break;
                    }
                }

                if (lineEnd >= 0)
                {
                    var lineContent = text.Substring(i, lineEnd - i);
                    _lineBuffer.Append(lineContent);
                    var completeLine = _lineBuffer.ToString();
                    _lineBuffer.Clear();

                    if (completeLine == string.Empty)
                    {
                        if (_hasData)
                        {
                            var data = _dataAccumulator.ToString();
                            if (data.Length > 0 && data[^1] == '\n')
                            {
                                data = data[..^1];
                            }

                            var evt = new ServerSentEvent(
                                Data: data,
                                EventType: _eventType ?? "message",
                                Id: _id,
                                Retry: _retry);
                            _pending.Enqueue(evt);
                        }

                        ResetEvent();
                    }
                    else if (!completeLine.StartsWith(':'))
                    {
                        ProcessField(completeLine);
                    }

                    i = lineEnd + endLength;
                }
                else
                {
                    var remaining = text[i..];
                    _lineBuffer.Append(remaining);
                    break;
                }
            }
        }

        private void ProcessField(string line)
        {
            string fieldName;
            string fieldValue;

            var colonIndex = line.IndexOf(':');
            if (colonIndex < 0)
            {
                fieldName = line;
                fieldValue = string.Empty;
            }
            else
            {
                fieldName = line[..colonIndex];
                var valueStart = colonIndex + 1;

                if (valueStart < line.Length && line[valueStart] == ' ')
                {
                    valueStart++;
                }

                fieldValue = valueStart < line.Length ? line[valueStart..] : string.Empty;
            }

            switch (fieldName)
            {
                case "data":
                    if (_dataAccumulator.Length > 0)
                    {
                        _dataAccumulator.Append('\n');
                    }
                    _dataAccumulator.Append(fieldValue);
                    _hasData = true;
                    break;

                case "event":
                    _eventType = fieldValue;
                    break;

                case "id":
                    if (!fieldValue.Contains('\0'))
                    {
                        _id = fieldValue;
                    }
                    break;

                case "retry":
                    if (int.TryParse(fieldValue, out var retryMs))
                    {
                        _retry = TimeSpan.FromMilliseconds(retryMs);
                    }
                    break;
            }
        }

        private void ResetEvent()
        {
            _dataAccumulator.Clear();
            _eventType = null;
            _id = null;
            _retry = null;
            _hasData = false;
        }
    }
}
