using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Text;
using System.Runtime.Intrinsics;
using System.Numerics;
using System.Runtime.Intrinsics.X86;
using System.Buffers.Text;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Reflection;
using System;
using System.Xml.Linq;
using System.Threading;

namespace _1brc;

public static unsafe class Brc
{
    public static void Main(string[] args)
    {
        CommandLineArgs parsedArgs = CommandLineArgs.Parse(args);
        if(parsedArgs.FilePath == null)
        {
            Console.WriteLine("Usage: 1brc [--timings] [--quiet] [--threads <num>] [--io (RA|MM)] <path_to_measurements_file>");
            Console.WriteLine("--threads - set an explicit number of threads to use. By default it uses all available hardware threads");
            Console.WriteLine("--io      - set the strategy used for IO: RA for random access or MM for memory mapped");
            Console.WriteLine("--timings - shows a self-measurement of execution time that won't include some process startup and shutdown costs");
            Console.WriteLine("--quiet   - doesn't print the standard list of station names and temperature statistics");
            return;
        }

        var sw = Stopwatch.StartNew();
        string resultText = ProcessFile(parsedArgs.FilePath, parsedArgs.Threads, parsedArgs.IOStrategy);
        sw.Stop();
        if(!parsedArgs.QuietMode)
        {
            Console.WriteLine(resultText);
        }
        if(parsedArgs.ShowTimings)
        {
            Console.WriteLine($"Processed in {sw.Elapsed}");
        }
    }

    public enum IOStrategy
    {
        RandomAccess,
        MemoryMapped
    }

    class CommandLineArgs
    {
        public bool ShowTimings;
        public bool QuietMode;
        public int? Threads;
        public string? FilePath;
        public IOStrategy? IOStrategy;

        public static CommandLineArgs Parse(string[] args)
        {
            CommandLineArgs parsedArgs = new CommandLineArgs();
            for(int i = 0; i < args.Length; i++)
            {
                string arg = args[i];
                if(arg == "--timings")
                {
                    parsedArgs.ShowTimings = true;
                }
                else if(arg == "--quiet")
                {
                    parsedArgs.QuietMode = true;
                }
                else if(arg == "--threads")
                {
                    i++;
                    string numThreads = args[i];
                    parsedArgs.Threads = int.Parse(numThreads); 
                }
                else if(arg == "--io")
                {
                    i++;
                    IOStrategy io = args[i] switch
                    {
                        "RA" => Brc.IOStrategy.RandomAccess,
                        "MM" => Brc.IOStrategy.MemoryMapped,
                        _ => throw new FormatException("Expected either RA or MM")
                    };
                    parsedArgs.IOStrategy = io;
                }
                else
                {
                    if(parsedArgs.FilePath != null)
                    {
                        throw new Exception("Unrecognized command line arg: " + arg);
                    }
                    parsedArgs.FilePath = arg;
                }
            }
            return parsedArgs;
        }
    }

    public static string ProcessFile(string filePath, int? threadCount = null, IOStrategy? ioOverride = null)
    {
        IOStrategy stategy = OperatingSystem.IsWindows() ? IOStrategy.RandomAccess : IOStrategy.MemoryMapped;
        stategy = ioOverride ?? stategy;
        using var chunkedIO = stategy switch
        {
            IOStrategy.RandomAccess => (IChunkedIO) new RandomAccessIO(filePath),
            _                       => (IChunkedIO) new MemoryMappedIO(filePath)
        };
            
        return ProcessFile(chunkedIO, threadCount);
    }

    public static string ProcessFile(IChunkedIO chunkedIO, int? threadCount = null)
    {
        if(!threadCount.HasValue)
        {
            threadCount = Environment.ProcessorCount;
#if DEBUG
            threadCount = 1;
#endif
        }

        return ProcessFile(threadCount.Value, chunkedIO, new PickStrategyChunkParser());
    }

    public static string ProcessFile(int threadCount, IChunkedIO chunkedIO, IChunkParser chunkParser)
    {
        Dictionary<string, Stats> dict = ParseFile(threadCount, chunkedIO, chunkParser);
        DumpNameStats(dict);
        return FormatResults(dict);
    }

    public static string FormatResults(Dictionary<string, Stats> results)
    {
        StringBuilder output = new StringBuilder();
        output.Append("{");
        IEnumerable<string> stations = results
            .OrderBy(kv => kv.Key, StringComparer.Ordinal)
            .Select(kv => $"{kv.Key}={kv.Value}");
        output.Append(string.Join(", ", stations));
        output.Append("}\n");
        return output.ToString();
    }

    public static void DumpNameStats(Dictionary<string,Stats> results)
    {
        IEnumerable<string> names = results.Select(kv => kv.Key).ToArray();
        var nameLengthGroups = names.GroupBy(n => n.Length).OrderByDescending(ng => ng.Key);
        int[] namesLessThan = new int[100];
        foreach(var ng in nameLengthGroups)
        {
            for(int i = 0; i < 100; i++)
            {
                if(ng.Key <= i)
                {
                    namesLessThan[i] += ng.Count();
                }
            }
        }
        Console.WriteLine("Names Less than X chars");
        for(int i = 0; i < 100; i++)
        {
            Console.WriteLine($"{i}: {namesLessThan[i]}");
        }
        Console.WriteLine();


        var nameGroups = names.GroupBy(name => GetNameHashBucket(name)).OrderByDescending(g => g.Count());
        Console.WriteLine("Hash count: " + nameGroups.Count());
        var hashGroups = nameGroups.GroupBy(ng => ng.Count()).OrderByDescending(ng => ng.Key);
        Console.WriteLine("Collision count - # hashes");
        foreach (var g in hashGroups)
        {
            Console.WriteLine($"{g.Key}: {g.Count()}");
        }
    }

    private static int GetNameHashBucket(string name)
    {
        byte[] bytes = new byte[100];
        Encoding.UTF8.GetBytes(name, bytes.AsSpan());
        long nameBytes = Unsafe.As<byte, long>(ref bytes[0]);
        return (int)((nameBytes * 0x353a6569c53a6569) >> 23) & ((1 << 15)-1);
    }

    public static Dictionary<string, Stats> ParseFile(int threadCount, IChunkedIO chunkedIO, IChunkParser chunkParser)
    {
        Task<IStationDictionary>[] workers = new Task<IStationDictionary>[threadCount];
        for (int i = 0; i < workers.Length; i++)
        {
            workers[i] = Task.Run(() => ParseFileChunksWorker(chunkedIO, chunkParser));
        }

        // merge results back into a single dictionary
        Dictionary<string, Stats> finalStats = new Dictionary<string, Stats>();
        for (int i = 0; i < workers.Length; i++)
        {
            foreach (KeyValuePair<string, Stats> kvp in workers[i].Result.GetEntries())
            {
                ref Stats stats = ref CollectionsMarshal.GetValueRefOrAddDefault(finalStats, kvp.Key, out bool exists);
                if(!exists)
                {
                    stats.Init();
                }
                stats.Merge(kvp.Value);
            }
        }
        return finalStats;
    }

    static unsafe IStationDictionary ParseFileChunksWorker(IChunkedIO chunkedIO, IChunkParser chunkParser)
    {
        if(!Debugger.IsAttached)
        {
            Thread.CurrentThread.Priority = ThreadPriority.Highest;
        }
        IStationDictionary dict = chunkParser.CreateDictionary();
        while (chunkedIO.TryGetNextChunk(out IntPtr chunkStart, out int chunkLength))
        {
            chunkParser.Parse(ref dict, chunkStart, chunkLength);
        }
        return dict;
    }
}