using System;
using System.Buffers.Text;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using System.Text;
using System.Threading.Tasks;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace _1brc
{
    public interface IChunkParser
    {
        IStationDictionary CreateDictionary();

        /// <summary>
        /// Read all rows contained in [buffer, buffer+length)
        /// buffer points to the start of a row
        /// buffer + length - 1 is the end of a row
        /// </summary>
        void Parse(ref IStationDictionary dictionary, IntPtr buffer, int length);
    }

    public unsafe class PickStrategyChunkParser : IChunkParser
    {
        SmallNameMultilineChunkParser _smallDataSetParser = new SmallNameMultilineChunkParser();
        LargeNameMultilineChunkParser _largeDataSetParser;

        public PickStrategyChunkParser(int prefetchLookAhead = 0)
        {
            _smallDataSetParser = new SmallNameMultilineChunkParser();
            _largeDataSetParser = new LargeNameMultilineChunkParser(prefetchLookAhead);
        }

        public IStationDictionary CreateDictionary() => new SparseDictionary();

        public void Parse(ref IStationDictionary dictionary, nint buffer, int length)
        {
            if(dictionary.Count < 500)
            {
                _smallDataSetParser.Parse(ref dictionary, buffer, length);
            }
            else
            {
                if(dictionary is SparseDictionary)
                {
                    // putting a lot of data into the sparse dictionary doesn't perform as well
                    // so we switch to the compact dictionary
                    dictionary = SwitchToCompactDictionary(dictionary);
                }
                _largeDataSetParser.Parse(ref dictionary, buffer, length);
            }
        }

        IStationDictionary SwitchToCompactDictionary(IStationDictionary sparseDictionary)
        {
            CompactDictionary newDictionary = new CompactDictionary();
            foreach (var kv in sparseDictionary.GetEntries())
            {
                Span<byte> utf8Name = Encoding.UTF8.GetBytes(kv.Key);
                Stats* stats = newDictionary.GetOrCreate(utf8Name);
                stats->Merge(kv.Value);
            }
            return newDictionary;
        }
    }

    public unsafe class MultilineChunkParserBase
    {
        const int LongestLegalRow = 100 + 7;

        Vector256<byte> _dummy;
        public MultilineChunkParserBase()
        {
            _dummy = Vector256<byte>.Zero.MaskLeftBytes(16);
            ParseQuadFixedPoint(Vector256<long>.Zero, out var unused, out var unused2);
        }

        protected void Partition(IntPtr bufferPtr, int length, int count, out long[] starts, out long[] ends)
        {
            starts = new long[count];
            ends = new long[count];
            long end = bufferPtr;
            for (int i = 0; i < count; i++)
            {
                long start = end;
                end = bufferPtr + length * (i + 1) / count;
                if (i == count - 1)
                {
                    end = bufferPtr + length - LongestLegalRow;
                }
                for (; *(byte*)(end - 1) != '\n'; end++) ;
                starts[i] = start;
                ends[i] = end;
            }
        }

        static readonly Vector256<byte> s_quadFixedPointLeftAlignShuffle = Vector256.Create(
            (byte)0, 2, 3, 4, 3, 2, 1, 0,
            8, 10, 11, 12, 11, 10, 9, 8,
            16, 18, 19, 20, 19, 18, 17, 16,
            24, 26, 27, 28, 27, 26, 25, 24);
        static readonly Vector256<byte> s_dotMask = Vector256.Create(
            0, (byte)'.', (byte)'.', 0, 0, 0, 0, 0,
            0, (byte)'.', (byte)'.', 0, 0, 0, 0, 0,
            0, (byte)'.', (byte)'.', 0, 0, 0, 0, 0,
            0, (byte)'.', (byte)'.', 0, 0, 0, 0, 0);
        static readonly Vector256<sbyte> s_dotMult = Vector256.Create(
            3, 2, 0, 0, 0, 0, 0, 0,
            3, 2, 0, 0, 0, 0, 0, 0,
            3, 2, 0, 0, 0, 0, 0, 0,
            3, 2, 0, 0, 0, 0, 0, 0);
        static readonly Vector256<sbyte> s_fixedPointMult1LeftAligned = Vector256.Create(
            1, 0, 10, 100, 0, 0, 0, 0,
            1, 0, 10, 100, 0, 0, 0, 0,
            1, 0, 10, 100, 0, 0, 0, 0,
            1, 0, 10, 100, 0, 0, 0, 0);

        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
        protected void ParseQuadFixedPoint(Vector256<long> tempUtf8Bytes, out Vector256<short> fixedPointTemps, out Vector256<long> dotPositions)
        {
            Vector256<byte> v = Avx2.Shuffle(tempUtf8Bytes.AsByte(), s_quadFixedPointLeftAlignShuffle);
            Vector256<byte> dashMask = Vector256.Create((long)'-').AsByte();
            Vector256<byte> dashes = Vector256.Equals(v, dashMask);
            Vector256<int> negMask = Vector256.ShiftRightArithmetic(Vector256.ShiftLeft(dashes.AsInt32(), 24), 24);
            Vector256<byte> dots = Avx2.ShiftRightLogical(Vector256.Equals(v, s_dotMask).AsInt64(), 8).AsByte();
            dotPositions = Avx2.And(Avx2.MultiplyAddAdjacent(dots, s_dotMult).AsInt64(), Vector256.Create(3L));
            Vector256<ulong> shifts = Avx2.ShiftLeftLogical(Vector256.Create(5L) - dotPositions, 3).AsUInt64();
            Vector256<byte> alignedV = Avx2.ShiftRightLogicalVariable(v.AsInt64(), shifts).AsByte();
            Vector256<byte> digits = Avx2.SubtractSaturate(alignedV, Vector256.Create<byte>((byte)'0'));
            Vector256<short> partialSums = Avx2.MultiplyAddAdjacent(digits, s_fixedPointMult1LeftAligned);
            Vector256<short> absFixedPoint = Vector256.Add(Avx2.ShiftRightLogical(partialSums.AsInt32(), 16).AsInt16(), partialSums);
            var negFixedPoint = -absFixedPoint;
            fixedPointTemps = Avx2.BlendVariable(absFixedPoint, negFixedPoint, negMask.AsInt16());
        }
    }

    public unsafe class SmallNameMultilineChunkParser : MultilineChunkParserBase, IChunkParser
    {
        ChunkParser<SparseDictionary> _fallbackParser = new ChunkParser<SparseDictionary>();

        public IStationDictionary CreateDictionary() => new SparseDictionary();

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public void Parse(ref IStationDictionary dictionary, IntPtr bufferPtr, int length)
        {
            SparseDictionary d = (SparseDictionary)dictionary;
            Partition(bufferPtr, length, 8, out long[] starts, out long[] ends);
            byte* chunkEnd = (byte*)bufferPtr + length;
            var decimalVector = Vector256.Create((byte)'.');
            var semicolonVector = Vector256.Create((byte)';');
            Vector256<long> cursorsA = Vector256.Create(starts);
            Vector256<long> cursorEndsA = Vector256.Create(ends);
            Vector256<long> cursorsB = Vector256.Create<long>(starts.AsSpan().Slice(4));
            Vector256<long> cursorEndsB = Vector256.Create<long>(ends.AsSpan().Slice(4));
            while (Avx2.And(Avx2.CompareGreaterThan(cursorEndsA, cursorsA),Avx2.CompareGreaterThan(cursorEndsB, cursorsB))
                .ExtractMostSignificantBits() == 0xF)
            {
                byte* cursor1 = (byte*)cursorsA.GetElement(0);
                byte* cursor2 = (byte*)cursorsA.GetElement(1);
                byte* cursor3 = (byte*)cursorsA.GetElement(2);
                byte* cursor4 = (byte*)cursorsA.GetElement(3);
                var data = Vector256.Load(cursor1);
                int semiIndex1 = data.IndexOf(semicolonVector);
                var data2 = Vector256.Load(cursor2);
                int semiIndex2 = data2.IndexOf(semicolonVector);
                var data3 = Vector256.Load(cursor3);
                int semiIndex3 = data3.IndexOf(semicolonVector);
                var data4 = Vector256.Load(cursor4);
                int semiIndex4 = data4.IndexOf(semicolonVector);
                var cursor1B = (byte*)cursorsB.GetElement(0);
                var cursor2B = (byte*)cursorsB.GetElement(1);
                var cursor3B = (byte*)cursorsB.GetElement(2);
                var cursor4B = (byte*)cursorsB.GetElement(3);
                var dataB = Vector256.Load(cursor1B);
                var semiIndex1B = dataB.IndexOf(semicolonVector);
                var data2B = Vector256.Load(cursor2B);
                var semiIndex2B = data2B.IndexOf(semicolonVector);
                var data3B = Vector256.Load(cursor3B);
                var semiIndex3B = data3B.IndexOf(semicolonVector);
                var data4B = Vector256.Load(cursor4B);
                var semiIndex4B = data4B.IndexOf(semicolonVector);
                if ((semiIndex1 | semiIndex2 | semiIndex3 | semiIndex4 | 
                    semiIndex1B | semiIndex2B | semiIndex3B | semiIndex4B) >= 32)

                {
                    break;
                }

                Vector256<byte> name1 = data.MaskLeftBytes(semiIndex1);
                Vector256<byte> name2 = data2.MaskLeftBytes(semiIndex2);
                Vector256<byte> name3 = data3.MaskLeftBytes(semiIndex3);
                Vector256<byte> name4 = data4.MaskLeftBytes(semiIndex4);
                Vector256<long> tempTexts = Vector256.Create(*(long*)(cursor1 + semiIndex1 + 1),
                                                             *(long*)(cursor2 + semiIndex2 + 1),
                                                             *(long*)(cursor3 + semiIndex3 + 1),
                                                             *(long*)(cursor4 + semiIndex4 + 1));
                ParseQuadFixedPoint(tempTexts, out Vector256<short> temps, out Vector256<long> dotPositions);
                Vector256<long> semiIndicies = Vector256.Create(semiIndex1, semiIndex2, semiIndex3, semiIndex4);
                cursorsA += semiIndicies + dotPositions + Vector256.Create(5L);

                Vector256<byte> name1B = dataB.MaskLeftBytes(semiIndex1B);
                Vector256<byte> name2B = data2B.MaskLeftBytes(semiIndex2B);
                Vector256<byte> name3B = data3B.MaskLeftBytes(semiIndex3B);
                Vector256<byte> name4B = data4B.MaskLeftBytes(semiIndex4B);
                var tempTextsB = Vector256.Create(*(long*)(cursor1B + semiIndex1B + 1),
                                                             *(long*)(cursor2B + semiIndex2B + 1),
                                                             *(long*)(cursor3B + semiIndex3B + 1),
                                                             *(long*)(cursor4B + semiIndex4B + 1));
                ParseQuadFixedPoint(tempTextsB, out var tempsB, out var dotPositionsB);
                var semiIndiciesB = Vector256.Create(semiIndex1B, semiIndex2B, semiIndex3B, semiIndex4B);
                cursorsB += semiIndiciesB + dotPositionsB + Vector256.Create(5L);

                Stats* stats1 = d.GetOrCreate(name1, semiIndex1);
                Stats* stats2 = d.GetOrCreate(name2, semiIndex2);
                Stats* stats3 = d.GetOrCreate(name3, semiIndex3);
                Stats* stats4 = d.GetOrCreate(name4, semiIndex4);
                Stats* stats1B = d.GetOrCreate(name1B, semiIndex1B);
                Stats* stats2B = d.GetOrCreate(name2B, semiIndex2B);
                Stats* stats3B = d.GetOrCreate(name3B, semiIndex3B);
                Stats* stats4B = d.GetOrCreate(name4B, semiIndex4B);
                stats1->InsertMeasurement(temps[0]);
                stats2->InsertMeasurement(temps[4]);
                stats3->InsertMeasurement(temps[8]);
                stats4->InsertMeasurement(temps[12]);
                stats1B->InsertMeasurement(tempsB[0]);
                stats2B->InsertMeasurement(tempsB[4]);
                stats3B->InsertMeasurement(tempsB[8]);
                stats4B->InsertMeasurement(tempsB[12]);
            }
            _fallbackParser.Parse(ref dictionary, (IntPtr)cursorsA[0], (int)(cursorEndsA[0] - cursorsA[0]));
            _fallbackParser.Parse(ref dictionary, (IntPtr)cursorsA[1], (int)(cursorEndsA[1] - cursorsA[1]));
            _fallbackParser.Parse(ref dictionary, (IntPtr)cursorsA[2], (int)(cursorEndsA[2] - cursorsA[2]));
            _fallbackParser.Parse(ref dictionary, (IntPtr)cursorsA[3], (int)(cursorEndsA[3] - cursorsA[3]));
            _fallbackParser.Parse(ref dictionary, (IntPtr)cursorsB[0], (int)(cursorEndsB[0] - cursorsB[0]));
            _fallbackParser.Parse(ref dictionary, (IntPtr)cursorsB[1], (int)(cursorEndsB[1] - cursorsB[1]));
            _fallbackParser.Parse(ref dictionary, (IntPtr)cursorsB[2], (int)(cursorEndsB[2] - cursorsB[2]));
            _fallbackParser.Parse(ref dictionary, (IntPtr)cursorsB[3], (int)(chunkEnd - cursorsB[3]));
        } 
    }

    public unsafe class LargeNameMultilineChunkParser : MultilineChunkParserBase, IChunkParser
    {
        const int LongestLegalRow = 100 + 7;
        ChunkParser<CompactDictionary> _fallbackParser = new ChunkParser<CompactDictionary>();
        int _prefetchLookupAhead;

        public LargeNameMultilineChunkParser(int prefetchLookAhead = 0)
        {
            _prefetchLookupAhead = prefetchLookAhead;
        }

        public IStationDictionary CreateDictionary() => new CompactDictionary();

        public void Parse(ref IStationDictionary dictionary, nint buffer, int length)
        {
            CompactDictionary d = (CompactDictionary)dictionary;
            Partition(buffer, length, 8, out long[] starts, out long[] ends);
            byte* chunkEnd = (byte*)buffer + length;
            var decimalVector = Vector256.Create((byte)'.');
            var semicolonVector = Vector256.Create((byte)';');
            Vector256<long> cursorsA = Vector256.Create(starts);
            Vector256<long> cursorEndsA = Vector256.Create(ends);
            Vector256<long> cursorsB = Vector256.Create<long>(starts.AsSpan().Slice(4));
            Vector256<long> cursorEndsB = Vector256.Create<long>(ends.AsSpan().Slice(4));
            while (Avx2.And(Avx2.CompareGreaterThan(cursorEndsA, cursorsA), Avx2.CompareGreaterThan(cursorEndsB, cursorsB))
                .ExtractMostSignificantBits() == 0xF)
            {
                byte* cursor1 = (byte*)cursorsA.GetElement(0);
                byte* cursor2 = (byte*)cursorsA.GetElement(1);
                byte* cursor3 = (byte*)cursorsA.GetElement(2);
                byte* cursor4 = (byte*)cursorsA.GetElement(3);
                var data = Vector256.Load(cursor1);
                int semiIndex1 = data.IndexOf(semicolonVector);
                var data2 = Vector256.Load(cursor2);
                int semiIndex2 = data2.IndexOf(semicolonVector);
                var data3 = Vector256.Load(cursor3);
                int semiIndex3 = data3.IndexOf(semicolonVector);
                var data4 = Vector256.Load(cursor4);
                int semiIndex4 = data4.IndexOf(semicolonVector);
                var cursor1B = (byte*)cursorsB.GetElement(0);
                var cursor2B = (byte*)cursorsB.GetElement(1);
                var cursor3B = (byte*)cursorsB.GetElement(2);
                var cursor4B = (byte*)cursorsB.GetElement(3);
                var dataB = Vector256.Load(cursor1B);
                var semiIndex1B = dataB.IndexOf(semicolonVector);
                var data2B = Vector256.Load(cursor2B);
                var semiIndex2B = data2B.IndexOf(semicolonVector);
                var data3B = Vector256.Load(cursor3B);
                var semiIndex3B = data3B.IndexOf(semicolonVector);
                var data4B = Vector256.Load(cursor4B);
                var semiIndex4B = data4B.IndexOf(semicolonVector);

                if(_prefetchLookupAhead != 0)
                {
                    Avx.Prefetch0(cursor1 + _prefetchLookupAhead);
                    Avx.Prefetch0(cursor2 + _prefetchLookupAhead);
                    Avx.Prefetch0(cursor3 + _prefetchLookupAhead);
                    Avx.Prefetch0(cursor4 + _prefetchLookupAhead);
                    Avx.Prefetch0(cursor1B + _prefetchLookupAhead);
                    Avx.Prefetch0(cursor2B + _prefetchLookupAhead);
                    Avx.Prefetch0(cursor3B + _prefetchLookupAhead);
                    Avx.Prefetch0(cursor4B + _prefetchLookupAhead);
                }

                Vector256<byte> name1 = data.MaskLeftBytes(semiIndex1);
                Vector256<byte> name2 = data2.MaskLeftBytes(semiIndex2);
                Vector256<byte> name3 = data3.MaskLeftBytes(semiIndex3);
                Vector256<byte> name4 = data4.MaskLeftBytes(semiIndex4);
                Vector256<byte> name1B = dataB.MaskLeftBytes(semiIndex1B);
                Vector256<byte> name2B = data2B.MaskLeftBytes(semiIndex2B);
                Vector256<byte> name3B = data3B.MaskLeftBytes(semiIndex3B);
                Vector256<byte> name4B = data4B.MaskLeftBytes(semiIndex4B);

                EntryHeader* likelyEntry1 = d.GetLikelyEntry(name1);
                EntryHeader* likelyEntry2 = d.GetLikelyEntry(name2);
                EntryHeader* likelyEntry3 = d.GetLikelyEntry(name3);
                EntryHeader* likelyEntry4 = d.GetLikelyEntry(name4);
                EntryHeader* likelyEntry1B = d.GetLikelyEntry(name1B);
                EntryHeader* likelyEntry2B = d.GetLikelyEntry(name2B);
                EntryHeader* likelyEntry3B = d.GetLikelyEntry(name3B);
                EntryHeader* likelyEntry4B = d.GetLikelyEntry(name4B);

                Stats* stats1 = GetStatsAndSemiIndex(d, cursor1, name1, likelyEntry1, ref semiIndex1);
                Stats* stats2 = GetStatsAndSemiIndex(d, cursor2, name2, likelyEntry2, ref semiIndex2);
                Stats* stats3 = GetStatsAndSemiIndex(d, cursor3, name3, likelyEntry3, ref semiIndex3);
                Stats* stats4 = GetStatsAndSemiIndex(d, cursor4, name4, likelyEntry4, ref semiIndex4);
                Stats* stats1B = GetStatsAndSemiIndex(d, cursor1B, name1B, likelyEntry1B, ref semiIndex1B);
                Stats* stats2B = GetStatsAndSemiIndex(d, cursor2B, name2B, likelyEntry2B, ref semiIndex2B);
                Stats* stats3B = GetStatsAndSemiIndex(d, cursor3B, name3B, likelyEntry3B, ref semiIndex3B);
                Stats* stats4B = GetStatsAndSemiIndex(d, cursor4B, name4B, likelyEntry4B, ref semiIndex4B);

                Vector256<long> tempTexts = Vector256.Create(*(long*)(cursor1 + semiIndex1 + 1),
                                                             *(long*)(cursor2 + semiIndex2 + 1),
                                                             *(long*)(cursor3 + semiIndex3 + 1),
                                                             *(long*)(cursor4 + semiIndex4 + 1));
                ParseQuadFixedPoint(tempTexts, out Vector256<short> temps, out Vector256<long> dotPositions);
                Vector256<long> semiIndicies = Vector256.Create(semiIndex1, semiIndex2, semiIndex3, semiIndex4);
                cursorsA += semiIndicies + dotPositions + Vector256.Create(5L);
                var tempTextsB = Vector256.Create(*(long*)(cursor1B + semiIndex1B + 1),
                                                             *(long*)(cursor2B + semiIndex2B + 1),
                                                             *(long*)(cursor3B + semiIndex3B + 1),
                                                             *(long*)(cursor4B + semiIndex4B + 1));
                ParseQuadFixedPoint(tempTextsB, out var tempsB, out var dotPositionsB);
                var semiIndiciesB = Vector256.Create(semiIndex1B, semiIndex2B, semiIndex3B, semiIndex4B);
                cursorsB += semiIndiciesB + dotPositionsB + Vector256.Create(5L);

                stats1->InsertMeasurement(temps[0]);
                stats2->InsertMeasurement(temps[4]);
                stats3->InsertMeasurement(temps[8]);
                stats4->InsertMeasurement(temps[12]);
                stats1B->InsertMeasurement(tempsB[0]);
                stats2B->InsertMeasurement(tempsB[4]);
                stats3B->InsertMeasurement(tempsB[8]);
                stats4B->InsertMeasurement(tempsB[12]);
            }
            _fallbackParser.Parse(ref dictionary, (IntPtr)cursorsA[0], (int)(cursorEndsA[0] - cursorsA[0]));
            _fallbackParser.Parse(ref dictionary, (IntPtr)cursorsA[1], (int)(cursorEndsA[1] - cursorsA[1]));
            _fallbackParser.Parse(ref dictionary, (IntPtr)cursorsA[2], (int)(cursorEndsA[2] - cursorsA[2]));
            _fallbackParser.Parse(ref dictionary, (IntPtr)cursorsA[3], (int)(cursorEndsA[3] - cursorsA[3]));
            _fallbackParser.Parse(ref dictionary, (IntPtr)cursorsB[0], (int)(cursorEndsB[0] - cursorsB[0]));
            _fallbackParser.Parse(ref dictionary, (IntPtr)cursorsB[1], (int)(cursorEndsB[1] - cursorsB[1]));
            _fallbackParser.Parse(ref dictionary, (IntPtr)cursorsB[2], (int)(cursorEndsB[2] - cursorsB[2]));
            _fallbackParser.Parse(ref dictionary, (IntPtr)cursorsB[3], (int)(chunkEnd - cursorsB[3]));
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private static Stats* GetStatsAndSemiIndex(CompactDictionary d, byte* cursor, Vector256<byte> name1, EntryHeader* likelyEntry, ref int semicolonIndex)
        {
            Stats* stats = null;
            if (semicolonIndex < 32)
            {
                stats = d.GetOrCreate(name1, semicolonIndex, likelyEntry);
            }
            else
            {
                var semicolonVector = Vector256.Create((byte)';');
                var data2 = Vector256.Load(cursor + 32);
                int semicolonIndex2 = data2.IndexOf(semicolonVector);
                if (semicolonIndex2 < 32)
                {
                    Vector256<byte> maskedName2 = data2.MaskLeftBytes(semicolonIndex2);
                    semicolonIndex = 32 + semicolonIndex2;
                    stats = d.GetOrCreate(name1, maskedName2, Vector256<byte>.Zero, 0, semicolonIndex);
                }
                else
                {
                    var data3 = Vector256.Load(cursor + 64);
                    int semicolonIndex3 = data3.IndexOf(semicolonVector);
                    if (semicolonIndex3 < 32)
                    {
                        Vector256<byte> maskedName3 = data3.MaskLeftBytes(semicolonIndex3);
                        semicolonIndex = 64 + semicolonIndex3;
                        stats = d.GetOrCreate(name1, data2, maskedName3, 0, semicolonIndex);
                    }
                    else
                    {
                        var data4 = Vector256.Load(cursor + 96);
                        int semicolonIndex4 = data4.IndexOf(semicolonVector);
                        Vector256<byte> maskedName4 = data4.MaskLeftBytes(semicolonIndex4);
                        semicolonIndex = 96 + semicolonIndex4;
                        stats = d.GetOrCreate(name1, data2, data3, maskedName4.AsInt32()[0], semicolonIndex);
                    }
                }
            }
            return stats;
        }
    }

    public unsafe class ChunkParser<TDictionary> : IChunkParser where TDictionary : IStationDictionary, new()
    {
        const int LongestLegalRow = 100 + 7;
        UnpaddedChunkParser<TDictionary> _unpaddedParser = new UnpaddedChunkParser<TDictionary>();

        public IStationDictionary CreateDictionary() => new TDictionary();

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public void Parse(ref IStationDictionary dictionary, IntPtr buffer, int length)
        {
            TDictionary d = (TDictionary)dictionary;
            byte* cursor = (byte*)buffer;
            byte* fastParseEnd = cursor + length - LongestLegalRow;
            for (; *(fastParseEnd - 1) != '\n'; fastParseEnd++) ;
            byte* chunkEnd = cursor + length;
            var newlineVector = Vector256.Create((byte)'\n');
            var semicolonVector = Vector256.Create((byte)';');
            while (cursor < fastParseEnd)
            {
                var data = Vector256.Load(cursor);
                int semicolonIndex = data.IndexOf(semicolonVector);
                Stats* stats = null;
                if (semicolonIndex < 32)
                {
                    Vector256<byte> maskedName = data.MaskLeftBytes(semicolonIndex);
                    stats = d.GetOrCreate(maskedName, semicolonIndex);
                }
                else
                {
                    var data2 = Vector256.Load(cursor + 32);
                    int semicolonIndex2 = data2.IndexOf(semicolonVector);
                    if (semicolonIndex2 < 32)
                    {
                        Vector256<byte> maskedName2 = data2.MaskLeftBytes(semicolonIndex2);
                        semicolonIndex = 32 + semicolonIndex2;
                        stats = d.GetOrCreate(data, maskedName2, Vector256<byte>.Zero, 0, semicolonIndex);
                    }
                    else
                    {
                        var data3 = Vector256.Load(cursor + 64);
                        int semicolonIndex3 = data3.IndexOf(semicolonVector);
                        if (semicolonIndex3 < 32)
                        {
                            Vector256<byte> maskedName3 = data3.MaskLeftBytes(semicolonIndex3);
                            semicolonIndex = 64 + semicolonIndex3;
                            stats = d.GetOrCreate(data, data2, maskedName3, 0, semicolonIndex);
                        }
                        else
                        {
                            var data4 = Vector256.Load(cursor + 96);
                            int semicolonIndex4 = data4.IndexOf(semicolonVector);
                            Vector256<byte> maskedName4 = data4.MaskLeftBytes(semicolonIndex4);
                            semicolonIndex = 96 + semicolonIndex4;
                            stats = d.GetOrCreate(data, data2, data3, maskedName4.AsInt32()[0], semicolonIndex);
                        }
                    }
                }
                var fixedPoint = ParseFixedPoint(cursor + semicolonIndex + 1, out cursor);
                stats->InsertMeasurement(fixedPoint);
            }
            _unpaddedParser.Parse(ref dictionary, (IntPtr)cursor, (int)(chunkEnd - cursor));
        }

        public static short ParseFixedPoint(byte* tempText, out byte* nextRowStart)
        {
            // Borrowed this bit shifting fun from @nietras though I think it may
            // have originated with the algorithm described by https://curiouscoding.nl/posts/1brc/

            const long dotBits = 0x10101000;
            const long multiplier = (100 * 0x1000000 + 10 * 0x10000 + 1);

            var word = Unsafe.ReadUnaligned<long>(tempText);
            var invWord = ~word;
            var decimalSepPos = BitOperations.TrailingZeroCount(invWord & dotBits);
            var signed = (invWord << 59) >> 63;
            var designMask = ~(signed & 0xFF);
            var digits = ((word & designMask) << (28 - decimalSepPos)) & 0x0F000F0F00L;
            var absValue = ((digits * multiplier) >>> 32) & 0x3FF;
            var measurement = (short)((absValue ^ signed) - signed);
            nextRowStart = tempText + (decimalSepPos >> 3) + 3;
            return measurement;
        }
    }

    /// <summary>
    /// A slower fallback parser that doesn't make any assumptions that there are readable
    /// padding bytes outside the chunk boundaries.
    /// </summary>
    unsafe class UnpaddedChunkParser<TDictionary> : IChunkParser where TDictionary : IStationDictionary, new()
    {
        public IStationDictionary CreateDictionary() => new TDictionary();

        public void Parse(ref IStationDictionary dictionary, IntPtr chunkStart, int length)
        {
            TDictionary d = (TDictionary)dictionary;
            byte* cursor = (byte*)chunkStart;
            byte* chunkEnd = cursor + length;
            while (cursor < chunkEnd)
            {
                ParseRow(d, ref cursor, (int)(chunkEnd - cursor));
            }
        }

        private static void ParseRow(TDictionary dictionary, ref byte* rowCursor, int length)
        {
            ReadOnlySpan<byte> chunk = new ReadOnlySpan<byte>(rowCursor, length);
            int indexOfNewline = chunk.IndexOf((byte)'\n');
            int indexOfSemicolon = chunk.IndexOf((byte)';');
            ReadOnlySpan<byte> cityName = chunk.Slice(0, indexOfSemicolon);
            ReadOnlySpan<byte> tempText = chunk.Slice(indexOfSemicolon + 1, indexOfNewline - indexOfSemicolon - 1);
            bool success = Utf8Parser.TryParse(tempText, out float temp, out int unusedBytesConsumed);
            Debug.Assert(success);
            Stats* stats = dictionary.GetOrCreate(cityName);
            short fixedPointTemp = (short)(Math.Round(temp * 10F));
            Debug.Assert(fixedPointTemp > -1000 && fixedPointTemp < 1000);
            stats->InsertMeasurement(fixedPointTemp);
            rowCursor += indexOfNewline + 1;
        }
    }


    public unsafe static class SIMDHelpers
    {
        static byte* firstNMask = (byte*)GCHandle.Alloc(new byte[] {
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        000, 000, 000, 000, 000, 000, 000, 000, 000, 000, 000, 000, 000, 000, 000, 000,
        000, 000, 000, 000, 000, 000, 000, 000, 000, 000, 000, 000, 000, 000, 000, 000 }, GCHandleType.Pinned).AddrOfPinnedObject() + 32;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Vector256<byte> GetLeftMask(int length)
        {
            return Vector256.Create(new ReadOnlySpan<byte>(firstNMask - length, 32));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Vector256<byte> MaskLeftBytes(this Vector256<byte> data, int length)
        {
            return Avx2.And(GetLeftMask(length), data);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Vector256<byte> MaskLeftBytes(this Vector256<byte> data, int length, out Vector256<byte> mask)
        {
            mask = GetLeftMask(length);
            return Avx2.And(mask, data);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int IndexOf(this Vector256<byte> data, byte searchValue)
        {
            return IndexOf(Vector256.Create(searchValue), data);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int IndexOf(this Vector256<byte> searchConstant, Vector256<byte> data)
        {
            var sMatches = Vector256.Equals(data, searchConstant);
            uint sMask = sMatches.ExtractMostSignificantBits();
            return BitOperations.TrailingZeroCount(sMask);
        }

        public static string DumpUtf8(this Vector256<byte> v)
        {
            byte[] bytes = new byte[32];
            v.CopyTo(bytes);
            return Encoding.UTF8.GetString(bytes);
        }

        public static string DumpUtf8(this Vector128<byte> v)
        {
            byte[] bytes = new byte[16];
            v.CopyTo(bytes);
            return Encoding.UTF8.GetString(bytes);
        }
    }
}
