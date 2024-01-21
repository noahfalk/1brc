using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using _1brc;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Engines;
using System.Runtime.CompilerServices;

namespace Benchmarks
{
    internal class Program
    {
        static void Main(string[] args)
        {
            var summary = BenchmarkRunner.Run<Parse>();
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

        string GetSourceDir([CallerFilePath] string? dir = null) => dir!;

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
