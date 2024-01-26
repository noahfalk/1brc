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

        string resultText = ProcessFile(parsedArgs.FilePath, parsedArgs.Threads, parsedArgs.IOStrategy);
        if(!parsedArgs.QuietMode)
        {
            Console.WriteLine(resultText);
        }
        RecordTime(TimingName.ResultsPrinted);
        if (parsedArgs.ShowTimings)
        {
            PrintTimings();
        }
    }

    public enum TimingName
    {
        GlobalIOOpening,
        GlobalIOOpened,
        FirstWorkerThreadStart,
        LastWorkerThreadStart,
        FirstWorkerThreadComplete,
        LastWorkerThreadComplete,
        ResultsMerged,
        ResultsFormatted,
        GlobalIOClosed,
        ResultsPrinted,
        Count
    }

    static long[] s_timings = new long[(int)TimingName.Count];
    static Stopwatch s_stopwatch = Stopwatch.StartNew();

    public static void RecordTime(TimingName timingName)
    {
        Interlocked.CompareExchange(ref s_timings[(int)timingName], s_stopwatch.ElapsedTicks, 0);
    }

    public static void PrintTimings()
    {
        Console.WriteLine($"{"Timing",-30} {"Total ms",8:N1} {"Diff ms",8:N1}");
        for (int i = 0; i < s_timings.Length; i++)
        {
            double totalMs = new TimeSpan(s_timings[i]).TotalMilliseconds;
            double diffMs = totalMs - (i == 0 ? 0 : new TimeSpan(s_timings[i - 1]).TotalMilliseconds);
            Console.WriteLine($"{(TimingName)i,-30} {totalMs,8:N1} {diffMs,8:N1}");
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
        IOStrategy stategy = IOStrategy.RandomAccess;
        stategy = ioOverride ?? stategy;
        RecordTime(TimingName.GlobalIOOpening);
        var chunkedIO = stategy switch
        {
            IOStrategy.RandomAccess => (IChunkedIO) new RandomAccessIO(filePath),
            _                       => (IChunkedIO) new MemoryMappedIO(filePath)
        };
        RecordTime(TimingName.GlobalIOOpened);
            
        string resultText = ProcessFile(chunkedIO, threadCount);
        chunkedIO.Dispose();
        RecordTime(TimingName.GlobalIOClosed);
        return resultText;
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
        string formattedResults = FormatResults(dict);
        RecordTime(TimingName.ResultsFormatted);
        return formattedResults;
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

    public static Dictionary<string, Stats> ParseFile(int threadCount, IChunkedIO chunkedIO, IChunkParser chunkParser)
    {
        Task<IStationDictionary>[] workers = new Task<IStationDictionary>[threadCount];
        for (int i = 0; i < workers.Length; i++)
        {
            workers[i] = Task.Run(() => ParseFileChunksWorker(chunkedIO, chunkParser, threadCount));
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
        RecordTime(TimingName.ResultsMerged);
        return finalStats;
    }

    static int s_workersStarted = 0;
    static int s_workersCompleted = 0;

    static unsafe IStationDictionary ParseFileChunksWorker(IChunkedIO chunkedIO, IChunkParser chunkParser, int workerCount)
    {
        if(!Debugger.IsAttached)
        {
            Thread.CurrentThread.Priority = ThreadPriority.Highest;
        }
        int workersStarted = Interlocked.Increment(ref s_workersStarted);
        if(workersStarted == 1)
        {
            RecordTime(TimingName.FirstWorkerThreadStart);
        }
        if(workersStarted == workerCount)
        {
            RecordTime(TimingName.LastWorkerThreadStart);
        }
        

        IStationDictionary dict = chunkParser.CreateDictionary();
        while (chunkedIO.TryGetNextChunk(out IntPtr chunkStart, out int chunkLength))
        {
            chunkParser.Parse(ref dict, chunkStart, chunkLength);
        }


        int workersComplete = Interlocked.Increment(ref s_workersCompleted);
        if (workersComplete == 1)
        {
            RecordTime(TimingName.FirstWorkerThreadComplete);
        }
        if (workersComplete == workerCount)
        {
            RecordTime(TimingName.LastWorkerThreadComplete);
        }
        return dict;
    }
}