using Microsoft.Win32.SafeHandles;
using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;

namespace _1brc
{
    public interface IChunkedIO : IDisposable
    {
        bool TryGetNextChunk(out IntPtr chunkStart, out int chunkLength);
    }

    public unsafe class MemoryMappedIO : IChunkedIO
    {
        private readonly int _maxChunkSize;
        private readonly byte* _pointer;
        private readonly long _fileLength;
        private readonly MemoryMappedFile _mapping;
        private readonly MemoryMappedViewAccessor _viewAccessor;

        private long _fileBytesClaimed;

        public MemoryMappedIO(string filePath, int maxChunkSize = 1 << 25)
        {
            using (FileStream fs = File.Open(filePath, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                _fileLength = fs.Length;
            }
            _mapping = MemoryMappedFile.CreateFromFile(filePath, FileMode.Open, null, 0, MemoryMappedFileAccess.Read);
            _viewAccessor = _mapping.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read);
            _viewAccessor.SafeMemoryMappedViewHandle.AcquirePointer(ref _pointer);
            _maxChunkSize = maxChunkSize;
        }

        public void Dispose()
        {
            _mapping.Dispose();
            _viewAccessor.Dispose();
        }

        public bool TryGetNextChunk(out IntPtr chunkStart, out int chunkLength)
        {
            long start;
            lock (this)
            {
                if (_fileBytesClaimed == _fileLength)
                {
                    chunkStart = chunkLength = 0;
                    return false;
                }
                start = _fileBytesClaimed;
                chunkLength = (int)Math.Min(_maxChunkSize, _fileLength - start);
                for (; *(_pointer + start + chunkLength - 1) != '\n'; chunkLength++) ;
                _fileBytesClaimed += chunkLength;
            }
            Debug.Assert(start == 0 || *(_pointer + start - 1) == '\n');
            Debug.Assert(*(_pointer + start + chunkLength - 1) == '\n');
            chunkStart = (IntPtr)(_pointer + start);
            return true;
        }
    }

    public class RandomAccessIO : IChunkedIO
    {
        class ThreadState : IDisposable
        {
            public readonly SafeFileHandle FileHandle;
            public readonly GCHandle BufferHandle;
            public long ClaimedRegionStart;
            public int ClaimedRegionLength;

            public ThreadState(string filePath, int bufferSize)
            {
                FileHandle = File.OpenHandle(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, FileOptions.SequentialScan);
                BufferHandle = GCHandle.Alloc(new byte[bufferSize], GCHandleType.Pinned);
            }
            public void Dispose() { BufferHandle.Free(); FileHandle?.Dispose(); }
        }

        const int LongestLegalRow = 100 + 7;

        private readonly string _filePath;
        private readonly SafeFileHandle _fileHandle; 
        private readonly int _maxChunkSize;
        private readonly int _maxRegionClaimSize;
        
        private readonly ThreadLocal<ThreadState> t_state = new ThreadLocal<ThreadState>();
        private readonly long _fileLength;
        private long _fileBytesClaimed;


        public RandomAccessIO(string filePath, int maxChunkSize = 1 << 18, int maxRegionClaimSize = 1 << 25)
        {
            _filePath = filePath;
            _fileHandle = File.OpenHandle(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
            _maxChunkSize = maxChunkSize;
            _maxRegionClaimSize = maxRegionClaimSize;
            _fileLength = RandomAccess.GetLength(_fileHandle);
        }

        public void Dispose()
        {
            _fileHandle.Dispose();
            t_state.Dispose();
        }

        public bool TryGetNextChunk(out nint chunkStart, out int chunkLength)
        {
            if(!t_state.IsValueCreated)
            {
                t_state.Value = new ThreadState(_filePath, _maxChunkSize);
            }

            ThreadState ts = t_state.Value!;
            if(ts.ClaimedRegionLength == 0)
            {
                if(!TryGetNextRegion(out ts.ClaimedRegionStart, out ts.ClaimedRegionLength))
                {
                    chunkStart = chunkLength = 0;
                    return false;
                }
            }

            byte[] buffer = (byte[])ts.BufferHandle.Target!;
            Span<byte> readBuffer = buffer.AsSpan().Slice(0, Math.Min(buffer.Length, ts.ClaimedRegionLength));
            ReadAll(ts.FileHandle!, readBuffer, ts.ClaimedRegionStart);
            chunkLength = readBuffer.LastIndexOf((byte)'\n') + 1;
            ts.ClaimedRegionLength -= chunkLength;
            ts.ClaimedRegionStart += chunkLength;
            chunkStart = ts.BufferHandle.AddrOfPinnedObject();
            return true;
        }

        bool TryGetNextRegion(out long regionStart, out int regionLength)
        {
            Span<byte> lastLineBuffer = stackalloc byte[LongestLegalRow];
            while (true)
            {
                long claimed = _fileBytesClaimed;
                if (_fileBytesClaimed == _fileLength)
                {
                    regionStart = regionLength = 0;
                    return false;
                }
                long newClaimLength = _fileLength - claimed;
                if(newClaimLength > _maxRegionClaimSize)
                {
                    long readFileOffset = claimed + _maxRegionClaimSize - lastLineBuffer.Length;
                    ReadAll(_fileHandle, lastLineBuffer, readFileOffset);
                    newClaimLength = _maxRegionClaimSize - lastLineBuffer.Length + lastLineBuffer.LastIndexOf((byte)'\n') + 1;
                }
                if(Interlocked.CompareExchange(ref _fileBytesClaimed, _fileBytesClaimed + newClaimLength, claimed) == claimed)
                {
                    regionStart = claimed;
                    regionLength = (int)newClaimLength;
                    return true;
                }
            }
        }

        private static void ReadAll(SafeFileHandle fileHandle, Span<byte> buffer, long fileOffset)
        {
            while (buffer.Length > 0)
            {
                int bytesRead = RandomAccess.Read(fileHandle, buffer, fileOffset);
                fileOffset += bytesRead;
                buffer = buffer.Slice(bytesRead);
            }
        }
    }
}
