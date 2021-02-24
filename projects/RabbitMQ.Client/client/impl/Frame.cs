// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
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
// The MPL v2.0:
//
//---------------------------------------------------------------------------
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
//  Copyright (c) 2007-2020 VMware, Inc.  All rights reserved.
//---------------------------------------------------------------------------

using System;
using System.Buffers;
using System.IO;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

using RabbitMQ.Client.Exceptions;
using RabbitMQ.Util;

namespace RabbitMQ.Client.Impl
{
    internal static class Framing
    {
        /* +------------+---------+----------------+---------+------------------+
         * | Frame Type | Channel | Payload length | Payload | Frame End Marker |
         * +------------+---------+----------------+---------+------------------+
         * | 1 byte     | 2 bytes | 4 bytes        | x bytes | 1 byte           |
         * +------------+---------+----------------+---------+------------------+ */
        internal const int BaseFrameSize = 1 + 2 + 4 + 1;
        internal const int StartFrameType = 0;
        internal const int StartChannel = 1;
        internal const int StartPayloadSize = 3;
        private const int StartPayload = 7;

        internal static class Method
        {
            /* +----------+-----------+-----------+
             * | CommandId (combined) | Arguments |    
             * | Class Id | Method Id |           |
             * +----------+-----------+-----------+
             * | 4 bytes (combined)   | x bytes   |
             * | 2 bytes  | 2 bytes   |           |
             * +----------+-----------+-----------+ */
            public const int FrameSize = BaseFrameSize + 2 + 2;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static int WriteTo<T>(Span<byte> span, ushort channel, T method) where T : MethodBase
            {
                const int StartClassId = StartPayload;
                const int StartMethodArguments = StartClassId + 4;

                int payloadLength = method.WriteArgumentsTo(span.Slice(StartMethodArguments)) + 4;
                NetworkOrderSerializer.WriteUInt64(span, ((ulong)Constants.FrameMethod << 56) | ((ulong)channel << 40) | ((ulong)payloadLength << 8));
                NetworkOrderSerializer.WriteUInt32(span.Slice(StartClassId), (uint)method.ProtocolCommandId);
                span[payloadLength + StartPayload] = Constants.FrameEnd;
                return payloadLength + BaseFrameSize;
            }
        }

        internal static class Header
        {
            /* +----------+----------+-------------------+-----------+
             * | Class Id | (unused) | Total body length | Arguments |
             * +----------+----------+-------------------+-----------+
             * | 2 bytes  | 2 bytes  | 8 bytes           | x bytes   |
             * +----------+----------+-------------------+-----------+ */
            public const int FrameSize = BaseFrameSize + 2 + 2 + 8;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static int WriteTo<T>(Span<byte> span, ushort channel, T header, int bodyLength) where T : ContentHeaderBase
            {
                const int StartClassId = StartPayload;
                const int StartBodyLength = StartPayload + 4;
                const int StartHeaderArguments = StartPayload + 12;

                int payloadLength = 12 + header.WritePropertiesTo(span.Slice(StartHeaderArguments));
                NetworkOrderSerializer.WriteUInt64(span, ((ulong)Constants.FrameHeader << 56) | ((ulong)channel << 40) | ((ulong)payloadLength << 8));
                NetworkOrderSerializer.WriteUInt32(span.Slice(StartClassId), (uint)header.ProtocolClassId << 16); // The last 16 bytes (Weight) aren't used
                NetworkOrderSerializer.WriteUInt64(span.Slice(StartBodyLength), (ulong)bodyLength);
                span[payloadLength + StartPayload] = Constants.FrameEnd;
                return payloadLength + BaseFrameSize;
            }
        }

        internal static class BodySegment
        {
            /* +--------------+
             * | Body segment |
             * +--------------+
             * | x bytes      |
             * +--------------+ */
            public const int FrameSize = BaseFrameSize;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static int WriteTo(Span<byte> span, ushort channel, ReadOnlySpan<byte> body)
            {
                const int StartBodyArgument = StartPayload;
                NetworkOrderSerializer.WriteUInt64(span, ((ulong)Constants.FrameBody << 56) | ((ulong)channel << 40) | ((ulong)body.Length << 8));
                body.CopyTo(span.Slice(StartBodyArgument));
                span[StartPayload + body.Length] = Constants.FrameEnd;
                return body.Length + BaseFrameSize;
            }
        }

        internal static class Heartbeat
        {
            /* Empty frame */
            public const int FrameSize = BaseFrameSize;

            /// <summary>
            /// Compiler trick to directly refer to static data in the assembly, see here: https://github.com/dotnet/roslyn/pull/24621
            /// </summary>
            private static ReadOnlySpan<byte> Payload => new byte[]
            {
                Constants.FrameHeartbeat,
                0, 0, // channel
                0, 0, 0, 0, // payload length
                Constants.FrameEnd
            };

            public static Memory<byte> GetHeartbeatFrame()
            {
                // Is returned by SocketFrameHandler.WriteLoop
                byte[] buffer = ArrayPool<byte>.Shared.Rent(FrameSize);
                Payload.CopyTo(buffer);
                return new Memory<byte>(buffer, 0, FrameSize);
            }
        }
    }

    internal readonly ref struct InboundFrame
    {
        public readonly FrameType Type;
        public readonly int Channel;
        public readonly ReadOnlyMemory<byte> Payload;
        private readonly byte[] _rentedArray;

        private InboundFrame(FrameType type, int channel, ReadOnlyMemory<byte> payload, byte[] rentedArray)
        {
            Type = type;
            Channel = channel;
            Payload = payload;
            _rentedArray = rentedArray;
        }

        private static void ProcessProtocolHeader(ReadOnlySequence<byte> buffer)
        {
            Span<byte> protocolSpan = stackalloc byte[7];
            buffer.Slice(1, 7).CopyTo(protocolSpan);
            if (protocolSpan[0] != 'M' || protocolSpan[1] != 'Q' || protocolSpan[2] != 'P')
            {
                throw new MalformedFrameException("Invalid AMQP protocol header from server");
            }

            throw new PacketNotRecognizedException(protocolSpan[3], protocolSpan[4], protocolSpan[5], protocolSpan[6]);
        }

        internal static ValueTask<ReadOnlyMemory<byte>> TryReadFrameBytes(PipeReader reader)
        {
            if (!reader.TryRead(out ReadResult result))
            {
                return TryReadFrameBytesSlow(reader);
            }

            return new ValueTask<ReadOnlyMemory<byte>>(ExtractFrameBytesImpl(reader, result));
        }

        internal static async ValueTask<ReadOnlyMemory<byte>> TryReadFrameBytesSlow(PipeReader reader)
        {
            ReadResult result = await reader.ReadAsync().ConfigureAwait(false);
            return ExtractFrameBytesImpl(reader, result);
        }

        private static ReadOnlyMemory<byte> ExtractFrameBytesImpl(PipeReader reader, ReadResult result)
        {
            if (result.Buffer.IsEmpty)
            {
                throw new EndOfStreamException("Reached the end of the stream. Possible authentication failure.");
            }

            if (result.Buffer.Length < 8)
            {
                reader.AdvanceTo(result.Buffer.Start);
                return null;
            }

            if (result.Buffer.First.Span[0] == 'A')
            {
                ProcessProtocolHeader(result.Buffer);
            }

            Span<byte> payloadSizeBytes = stackalloc byte[4];
            result.Buffer.Slice(3, 4).CopyTo(payloadSizeBytes);
            int payloadSize = NetworkOrderDeserializer.ReadInt32(payloadSizeBytes); // FIXME - throw exn on unreasonable value
            if (result.Buffer.Length < payloadSize + 8)
            {
                reader.AdvanceTo(result.Buffer.Start);
                return null;
            }

            int frameSize = payloadSize + 8;
            byte[] payloadBytes = ArrayPool<byte>.Shared.Rent(frameSize);
            ReadOnlySequence<byte> slice = result.Buffer.Slice(0, frameSize);
            slice.CopyTo(payloadBytes);
            if (payloadBytes[frameSize - 1] != Constants.FrameEnd)
            {
                ArrayPool<byte>.Shared.Return(payloadBytes);
                throw new MalformedFrameException($"Bad frame end marker: {payloadBytes[payloadSize]}");
            }

            reader.AdvanceTo(slice.End);
            return payloadBytes.AsMemory(0, frameSize);
        }

        internal static InboundFrame ReadFrom(ReadOnlyMemory<byte> frameBytes)
        {
            MemoryMarshal.TryGetArray(frameBytes, out ArraySegment<byte> segment);
            return new InboundFrame((FrameType)frameBytes.Span[0], NetworkOrderDeserializer.ReadUInt16(frameBytes.Span.Slice(1)), frameBytes.Slice(7, frameBytes.Length - 8), segment.Array);
        }

        public byte[] TakeoverPayload()
        {
            return _rentedArray;
        }

        public void ReturnPayload()
        {
            ArrayPool<byte>.Shared.Return(_rentedArray);
        }

        public override string ToString()
        {
            return $"(type={Type}, channel={Channel}, {Payload.Length} bytes of payload)";
        }
    }

    internal enum FrameType : int
    {
        FrameMethod = Constants.FrameMethod,
        FrameHeader = Constants.FrameHeader,
        FrameBody = Constants.FrameBody,
        FrameHeartbeat = Constants.FrameHeartbeat
    }
}
