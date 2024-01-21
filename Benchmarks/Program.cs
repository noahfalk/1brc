using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using _1brc;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Engines;
using System.Runtime.CompilerServices;
using static Benchmarks.Parse;

namespace Benchmarks
{
    internal class Program
    {
        static void Main(string[] args)
        {
            var summary = BenchmarkRunner.Run<MemoryMapPrefetchLookAhead>();
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
