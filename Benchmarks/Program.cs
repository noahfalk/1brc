using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using _1brc;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Engines;
using System.Runtime.CompilerServices;
using static Benchmarks.Parse;
using System.Runtime.Intrinsics;
using System.Text;
using System.Diagnostics;

namespace Benchmarks
{
    internal class Program
    {
        static void Main(string[] args)
        {
            var summary = BenchmarkRunner.Run<IndexLookupThroughput>(new DebugInProcessConfig());
        }
    }


    [IterationCount(5)]
    [WarmupCount(1)]
    [EvaluateOverhead(false)]
    public class MemoryMapPrefetchLookAhead
    {
        private readonly string _filePath = "/root/git/1brc_data/measurements-10K.txt";

        [Params(0, 1, 2, 3, 4, 5, 6)]
        public int LinesAhead { get; set; }

        [Benchmark]
        public void ParseAndAccumulate() => Brc.ParseFile(Environment.ProcessorCount, new MemoryMappedIO(_filePath),
            new PickStrategyChunkParser(LinesAhead*64));
    }

    [IterationCount(5)]
    [WarmupCount(1)]
    [EvaluateOverhead(false)]
    public class MemoryMapChunkSize
    {
        private readonly string _filePath = "/root/git/1brc_data/measurements-10K.txt";

        [Params(16_000,32_000,64_000,90_000,128_000)]
        public int ChunkSize { get; set; }

        [Benchmark]
        public void ParseAndAccumulate() => Brc.ProcessFile(new MemoryMappedIO(_filePath, ChunkSize));
    }

    /*
    [HardwareCounters(
HardwareCounter.InstructionRetired,
HardwareCounter.TotalCycles,
HardwareCounter.BranchInstructionRetired,
HardwareCounter.BranchMispredictsRetired,
HardwareCounter.LlcMisses)] */
    [IterationCount(5)]
    [WarmupCount(1)]
    [EvaluateOverhead(false)]
    public class IndexLookupThroughput
    {
        public UtfName32[] GetNames(int nameCount)
        {
            UtfName32[] names = new UtfName32[nameCount];
            for (int i = 0; i < names.Length; i++)
            {
                byte[] nameBytes = new byte[32];
                for (int j = 0; j < 27; j++) { nameBytes[j] = (byte)('a'+j); }
                Encoding.UTF8.GetBytes(i.ToString(), nameBytes.AsSpan());
                names[i].NameBytes = Vector256.Create<byte>(nameBytes);
                names[i].Length = 27;
            }

            UtfName32[] bigNames = new UtfName32[10_000];
            for (int i = 0; i < bigNames.Length; i++)
            {
                bigNames[i] = names[i % nameCount];
            }
            return bigNames;
        }

        public IEnumerable<object[]> GetNameSets()
        {
            //yield return new object[] { GetNames(1), 1 };
            //yield return new object[] { GetNames(10), 10 };
            yield return new object[] { GetNames(100), 100 };
            yield return new object[] { GetNames(1000), 1000 };
            yield return new object[] { GetNames(10_000), 10_000 };
        }

        [Benchmark]
        [ArgumentsSource(nameof(GetNameSets))]
        public unsafe void LookupNames1M(UtfName32[] names, int nameCount)
        {
            Index32 d = new Index32();
            for (int j = 0; j < 100; j++)
            {
                if(d.NeedsRehash)
                {
                    d.Rehash();
                }
                for (int i = 0; i < names.Length; i++)
                {
                    int index = d.GetOrCreate(names[i]);
                    Debug.Assert(index == i % nameCount);
                }
            }
        }

        [Benchmark]
        [ArgumentsSource(nameof(GetNameSets))]
        public unsafe void CompactDictionaryLookup1M(UtfName32[] names, int nameCount)
        {
            CompactDictionary d = new CompactDictionary();
            for (int j = 0; j < 100; j++)
            {
                for (int i = 0; i < names.Length; i++)
                {
                    Stats* s = d.GetOrCreate(names[i].NameBytes, 27);
                }
            }
        }
    }

    [HardwareCounters(
    HardwareCounter.InstructionRetired,
    HardwareCounter.TotalCycles,
    HardwareCounter.BranchInstructionRetired,
    HardwareCounter.BranchMispredictsRetired,
    HardwareCounter.LlcMisses)]
    [IterationCount(5)]
    [WarmupCount(1)]
    [EvaluateOverhead(false)]
    public class DictionaryLookupThroughput
    {
        public Vector256<byte>[] GetNames(int nameCount)
        {
            Vector256<byte>[] names = new Vector256<byte>[nameCount];
            for(int i = 0; i < names.Length; i++)
            {
                byte[] nameBytes = new byte[32];
                for(int j = 0; j < 31; j++) { nameBytes[j] = (byte)'x'; }
                Encoding.UTF8.GetBytes(i.ToString(), nameBytes.AsSpan());
                names[i] = Vector256.Create<byte>(nameBytes);
            }

            Vector256<byte>[] bigNames = new Vector256<byte>[10_000];
            for(int i = 0;i < bigNames.Length; i++)
            {
                bigNames[i] = names[i % nameCount];
            }
            return bigNames;
        }

        public IEnumerable<object[]> GetNameSets()
        {
            yield return new object[] { GetNames(1), 1 };
            yield return new object[] { GetNames(10), 10 };
            yield return new object[] { GetNames(100), 100 };
            yield return new object[] { GetNames(1000), 1000 };
            yield return new object[] { GetNames(10_000), 10_000 };
        }

        [Benchmark]
        [ArgumentsSource(nameof(GetNameSets))]
        public unsafe void LookupNames1M(Vector256<byte>[] names, int nameCount)
        {
            CompactDictionary d = new CompactDictionary();
            for (int j = 0; j < 100; j++)
            {
                for (int i = 0; i < names.Length; i++)
                {
                    Stats* s = d.GetOrCreate(names[i], 31);
                }
            }
        }
    }

    /*
    [HardwareCounters(
    HardwareCounter.InstructionRetired,
    HardwareCounter.TotalCycles,
    HardwareCounter.BranchInstructionRetired,
    HardwareCounter.BranchMispredictsRetired,
    HardwareCounter.LlcMisses)] */
    [IterationCount(5)]
    [WarmupCount(1)]
    [EvaluateOverhead(false)]
    public class Parse
    {
        private readonly string _filePath = "/root/git/1brc_data/measurements-10K.txt";

        [Params("RandomAccess", "MemoryMapped")]
        public string? IOStrategy { get; set; }

        [Benchmark]
        public void ParseAndAccumulate() => Brc.ProcessFile(GetChunkedIO());

        [Benchmark]
        public void NoParseRead() =>
            Brc.ParseFile(Environment.ProcessorCount, GetChunkedIO(), new StrideChunkParser());


        IChunkedIO GetChunkedIO()
        {
            return IOStrategy switch
            {
                "RandomAccess" => new RandomAccessIO(_filePath),
                "MemoryMapped" => new MemoryMappedIO(_filePath),
                _ => throw new InvalidOperationException()
            };
        }

        unsafe public class StrideChunkParser : IChunkParser
        {
            public IStationDictionary CreateDictionary() => new SparseDictionary();
            public void Parse(ref IStationDictionary dict, IntPtr buffer, int length)
            {
                byte* ptr = (byte*)buffer;
                byte b = 0;
                for (int i = 0; i < length; i += 64)
                {
                    b |= *(ptr + i);
                }
            }
        }
    }
}
