// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 1.1.
//
// The APL v2.0:
//
//---------------------------------------------------------------------------
//   Copyright (c) 2007-2020 VMware, Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       https://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//---------------------------------------------------------------------------
//
// The MPL v1.1:
//
//---------------------------------------------------------------------------
//  The contents of this file are subject to the Mozilla Public License
//  Version 1.1 (the "License"); you may not use this file except in
//  compliance with the License. You may obtain a copy of the License
//  at https://www.mozilla.org/MPL/
//
//  Software distributed under the License is distributed on an "AS IS"
//  basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
//  the License for the specific language governing rights and
//  limitations under the License.
//
//  The Original Code is RabbitMQ.
//
//  The Initial Developer of the Original Code is Pivotal Software, Inc.
//  Copyright (c) 2007-2020 VMware, Inc.  All rights reserved.
//---------------------------------------------------------------------------

using System;
using System.Buffers;
using System.IO;
using System.Net.Sockets;

using RabbitMQ.Client.Exceptions;
using RabbitMQ.Client.Framing;
using RabbitMQ.Util;

namespace RabbitMQ.Client.Impl
{
    class HeaderOutboundFrame : OutboundFrame
    {
        private readonly ContentHeaderBase _header;
        private readonly int _bodyLength;

        internal HeaderOutboundFrame(int channel, ContentHeaderBase header, int bodyLength) : base(FrameType.FrameHeader, channel)
        {
            _header = header;
            _bodyLength = bodyLength;
        }

        internal override int GetMinimumPayloadBufferSize()
        {
            // ProtocolClassId (2) + header (X bytes)
            return 2 + _header.GetRequiredBufferSize();
        }

        internal override int WritePayload(Memory<byte> memory)
        {
            // write protocol class id (2 bytes)
            NetworkOrderSerializer.WriteUInt16(memory, _header.ProtocolClassId);
            // write header (X bytes)
            int bytesWritten = _header.WriteTo(memory.Slice(2), (ulong)_bodyLength);
            return 2 + bytesWritten;
        }
    }

    class BodySegmentOutboundFrame : OutboundFrame
    {
        private readonly byte[] _body;
        private readonly int _offset;
        private readonly int _count;

        public BodySegmentOutboundFrame(int channel, byte[] body, int offset, int count) : base(FrameType.FrameBody, channel)
        {
            _body = body;
            _offset = offset;
            _count = count;
        }

        internal override int GetMinimumPayloadBufferSize()
        {
            return _count;
        }

        internal override int WritePayload(Memory<byte> memory)
        {
            _body.AsMemory(_offset, _count).CopyTo(memory);
            return _count;
        }
    }

    class MethodOutboundFrame : OutboundFrame
    {
        private readonly MethodBase _method;

        internal MethodOutboundFrame(int channel, MethodBase method) : base(FrameType.FrameMethod, channel)
        {
            _method = method;
        }

        internal override int GetMinimumPayloadBufferSize()
        {
            // class id (2 bytes) + method id (2 bytes) + arguments (X bytes)
            return 4 + _method.GetRequiredBufferSize();
        }

        internal override int WritePayload(Memory<byte> memory)
        {
            NetworkOrderSerializer.WriteUInt16(memory, _method.ProtocolClassId);
            NetworkOrderSerializer.WriteUInt16(memory.Slice(2), _method.ProtocolMethodId);
            var argWriter = new MethodArgumentWriter(memory.Slice(4));
            _method.WriteArgumentsTo(argWriter);
            argWriter.Flush();
            return 4 + argWriter.Offset;
        }
    }

    class EmptyOutboundFrame : OutboundFrame
    {
        public EmptyOutboundFrame() : base(FrameType.FrameHeartbeat, 0)
        {
        }

        internal override int GetMinimumPayloadBufferSize()
        {
            return 0;
        }

        internal override int WritePayload(Memory<byte> memory)
        {
            return 0;
        }
    }

    abstract class OutboundFrame : Frame
    {
        public int ByteCount { get; private set; } = 0;
        public OutboundFrame(FrameType type, int channel) : base(type, channel)
        {
        }

        internal void WriteTo(Memory<byte> memory)
        {
            memory.Span[0] = (byte)Type;
            NetworkOrderSerializer.WriteUInt16(memory.Slice(1), (ushort)Channel);
            int bytesWritten = WritePayload(memory.Slice(7));
            NetworkOrderSerializer.WriteUInt32(memory.Slice(3), (uint)bytesWritten);
            memory.Span[bytesWritten + 7] = Constants.FrameEnd;
            ByteCount = bytesWritten + 8;
        }

        internal abstract int WritePayload(Memory<byte> memory);
        internal abstract int GetMinimumPayloadBufferSize();
        internal int GetMinimumBufferSize()
        {
            return 8 + GetMinimumPayloadBufferSize();
        }
    }

    class InboundFrame : Frame, IDisposable
    {
        public int PayloadSize { get; private set; }
        private InboundFrame(FrameType type, int channel, byte[] payload, int payloadSize) : base(type, channel, payload)
        {
            PayloadSize = payloadSize;
        }

        private static void ProcessProtocolHeader(NetworkBinaryReader reader)
        {
            try
            {
                byte b1 = reader.ReadByte();
                byte b2 = reader.ReadByte();
                byte b3 = reader.ReadByte();
                if (b1 != 'M' || b2 != 'Q' || b3 != 'P')
                {
                    throw new MalformedFrameException("Invalid AMQP protocol header from server");
                }

                int transportHigh = reader.ReadByte();
                int transportLow = reader.ReadByte();
                int serverMajor = reader.ReadByte();
                int serverMinor = reader.ReadByte();
                throw new PacketNotRecognizedException(transportHigh,
                    transportLow,
                    serverMajor,
                    serverMinor);
            }
            catch (EndOfStreamException)
            {
                // Ideally we'd wrap the EndOfStreamException in the
                // MalformedFrameException, but unfortunately the
                // design of MalformedFrameException's superclass,
                // ProtocolViolationException, doesn't permit
                // this. Fortunately, the call stack in the
                // EndOfStreamException is largely irrelevant at this
                // point, so can safely be ignored.
                throw new MalformedFrameException("Invalid AMQP protocol header from server");
            }
        }

        internal static InboundFrame ReadFrom(NetworkBinaryReader reader)
        {
            int type;

            try
            {
                type = reader.ReadByte();
            }
            catch (IOException ioe)
            {
                // If it's a WSAETIMEDOUT SocketException, unwrap it.
                // This might happen when the limit of half-open connections is
                // reached.
                if (ioe.InnerException == null ||
                    !(ioe.InnerException is SocketException) ||
                    ((SocketException)ioe.InnerException).SocketErrorCode != SocketError.TimedOut)
                {
                    throw ioe;
                }
                throw ioe.InnerException;
            }

            if (type == 'A')
            {
                // Probably an AMQP protocol header, otherwise meaningless
                ProcessProtocolHeader(reader);
            }

            using (IMemoryOwner<byte> headerMemory = MemoryPool<byte>.Shared.Rent(6))
            {
                Memory<byte> headerSlice = headerMemory.Memory.Slice(0, 6);
                reader.Read(headerSlice);
                int channel = NetworkOrderDeserializer.ReadUInt16(headerSlice);
                int payloadSize = NetworkOrderDeserializer.ReadInt32(headerSlice.Slice(2)); // FIXME - throw exn on unreasonable value
                byte[] payload = ArrayPool<byte>.Shared.Rent(payloadSize);
                int bytesRead = 0;
                try
                {
                    while (bytesRead < payloadSize)
                    {
                        bytesRead += reader.Read(payload, bytesRead, payloadSize - bytesRead);
                    }
                }
                catch (Exception)
                {
                    // Early EOF.
                    throw new MalformedFrameException($"Short frame - expected to read {payloadSize} bytes, only got {bytesRead} bytes");
                }

                int frameEndMarker = reader.ReadByte();
                if (frameEndMarker != Constants.FrameEnd)
                {
                    throw new MalformedFrameException("Bad frame end marker: " + frameEndMarker);
                }

                return new InboundFrame((FrameType)type, channel, payload, payloadSize);
            }
        }

        internal NetworkBinaryReader GetReader()
        {
            return new NetworkBinaryReader(new MemoryStream(base.Payload, 0, PayloadSize));
        }

        public void Dispose()
        {
            if (Payload != null)
            {
                ArrayPool<byte>.Shared.Return(Payload);
            }
        }
    }

    class Frame
    {
        public Frame(FrameType type, int channel)
        {
            Type = type;
            Channel = channel;
            Payload = null;
        }

        public Frame(FrameType type, int channel, byte[] payload)
        {
            Type = type;
            Channel = channel;
            Payload = payload;
        }

        public int Channel { get; private set; }

        public byte[] Payload { get; private set; }

        public FrameType Type { get; private set; }

        public override string ToString()
        {
            return string.Format("(type={0}, channel={1}, {2} bytes of payload)",
                Type,
                Channel,
                Payload == null
                    ? "(null)"
                    : Payload.Length.ToString());
        }

        public bool IsMethod()
        {
            return Type == FrameType.FrameMethod;
        }
        public bool IsHeader()
        {
            return Type == FrameType.FrameHeader;
        }
        public bool IsBody()
        {
            return Type == FrameType.FrameBody;
        }
        public bool IsHeartbeat()
        {
            return Type == FrameType.FrameHeartbeat;
        }
    }

    enum FrameType : int
    {
        FrameMethod = 1,
        FrameHeader = 2,
        FrameBody = 3,
        FrameHeartbeat = 8,
        FrameEnd = 206,
        FrameMinSize = 4096
    }

}
