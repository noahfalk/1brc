using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using System.Text;

namespace _1brc
{
    public unsafe struct EntryHeader
    {
        public Stats Value;
        public uint Next;
        public ushort Length;
        // UTF8 string data here
    }

    public unsafe sealed class CompactDictionary : IStationDictionary
    {
        const int Size = 1 << 16;
        uint* _buckets;
        byte* _entries;
        uint _nextEntryOffset;
        uint _entriesSize;

        public CompactDictionary()
        {
            _buckets = (uint*)Marshal.AllocHGlobal(Size*Unsafe.SizeOf<uint>());
            for (int i = 0; i < Size; i++) _buckets[i] = uint.MaxValue;
            _entriesSize = (uint)((100 + Unsafe.SizeOf<EntryHeader>()) * 10_000);
            _entries = (byte*)Marshal.AllocHGlobal((int)_entriesSize);
        }

        public int Count { get; private set; }

        public IEnumerable<KeyValuePair<string, Stats>> GetEntries()
        {
            List<KeyValuePair<string,Stats>> entries = new List<KeyValuePair<string,Stats>>();
            for(int i = 0; i < _nextEntryOffset;)
            {
                EntryHeader* entry = (EntryHeader*)(_entries + i);
                Span<byte> nameBuffer = new Span<byte>(_entries + i + Unsafe.SizeOf<EntryHeader>(), entry->Length);
                int terminator = nameBuffer.IndexOf((byte)0);
                if (terminator != -1)
                {
                    nameBuffer = nameBuffer.Slice(0, terminator);
                }
                string name = Encoding.UTF8.GetString(nameBuffer);
                entries.Add(new KeyValuePair<string, Stats>(name, entry->Value));
                i += entry->Length + Unsafe.SizeOf<EntryHeader>();
            }
            return entries;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public EntryHeader* GetLikelyEntry(Vector256<byte> name1)
        {
            int hash = GetHashCode(name1);
            int bucketIdx = hash & (Size - 1);
            uint bucket = _buckets[bucketIdx];
            EntryHeader* entry = null;
            if (bucket < _nextEntryOffset)
            {
                entry = (EntryHeader*)(_entries + bucket);
                Avx.Prefetch0(entry);
            }
            return entry;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Stats* GetOrCreate(Vector256<byte> name, int len)
        {
            Debug.Assert(len < 32);
            Debug.Assert(name[len] == 0);
            int hash = GetHashCode(name);
            int bucketIdx = hash & (Size - 1);
            uint entryOffset = _buckets[bucketIdx];
            EntryHeader* entry = null;
            while (entryOffset < _nextEntryOffset)
            {
                entry = (EntryHeader*)(_entries + entryOffset);
                if(IsMatch(entry, name, len))
                {
                    return &entry->Value;
                }
                entryOffset = entry->Next;
            }
            return Create(hash, entry, name);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Stats* GetOrCreate(Vector256<byte> name, int len, EntryHeader* likelyEntry)
        {
            Debug.Assert(len < 32);
            Debug.Assert(name[len] == 0);
            EntryHeader* entry = likelyEntry;
            if(entry != null)
            {
                if(IsMatch(entry, name, len))
                {
                    return &entry->Value;
                }
                uint entryOffset = entry->Next;
                while (entryOffset < _nextEntryOffset)
                {
                    entry = (EntryHeader*)(_entries + entryOffset);
                    if (IsMatch(entry, name, len))
                    {
                        return &entry->Value;
                    }
                    entryOffset = entry->Next;
                }
            }
            return Create(GetHashCode(name), entry, name);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        Stats* Create(int hash, EntryHeader* entry, Vector256<byte> name)
        {
            Debug.Assert(Count < 10_000);
            if (entry == null)
            {
                _buckets[hash & (Size - 1)] = _nextEntryOffset;
            }
            else
            {
                entry->Next = _nextEntryOffset;
            }
            EntryHeader* newEntry = (EntryHeader*)(_entries + _nextEntryOffset);
            InitEntry(newEntry, name);
            _nextEntryOffset += (uint)(Unsafe.SizeOf<EntryHeader>() + newEntry->Length);
            Count++;
            return &newEntry->Value;
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public Stats* GetOrCreate(Vector256<byte> name1, Vector256<byte> name2, Vector256<byte> name3, int name4, int len)
        {
            Debug.Assert(len <= 100);
            Debug.Assert(len >= 32 || name1[len] == 0);
            Debug.Assert(len >= 64 || len < 32 || name2[len-32] == 0);
            Debug.Assert(len >= 96 || len < 64 || name3[len-64] == 0);
            Debug.Assert(len == 100 || len < 96 || name4 >> (len-96)*8 == 0);
            int hash = GetHashCode(name1);
            int bucketIdx = hash & (Size - 1);
            uint entryOffset = _buckets[bucketIdx];
            EntryHeader* entry = null;
            while (entryOffset < _nextEntryOffset)
            {
                entry = (EntryHeader*)(_entries + entryOffset);
                if (IsMatch(entry, name1, name2, name3, name4))
                {
                    return &entry->Value;
                }
                entryOffset = entry->Next;
            }

            if (entry == null)
            {
                _buckets[bucketIdx] = _nextEntryOffset;
            }
            else
            {
                entry->Next = _nextEntryOffset;
            }
            EntryHeader* newEntry = (EntryHeader*)(_entries + _nextEntryOffset);
            InitEntry(newEntry, name1, name2, name3, name4);
            _nextEntryOffset += (uint)(Unsafe.SizeOf<EntryHeader>() + newEntry->Length);
            Count++;
            return &newEntry->Value;
        }

        public Stats* GetOrCreate(ReadOnlySpan<byte> name)
        {
            if (name.Length < 32)
            {
                return GetOrCreate(SpanToVector(name), name.Length);
            }
            else
            {
                Debug.Assert(name.Length <= 100);
                return GetOrCreate(SpanToVector(name),
                                   name.Length > 32 ? SpanToVector(name.Slice(32)) : Vector256<byte>.Zero,
                                   name.Length > 64 ? SpanToVector(name.Slice(64)) : Vector256<byte>.Zero,
                                   name.Length > 96 ? SpanToInt(name.Slice(96)) : 0,
                                   name.Length);
            }
        }

        static Vector256<byte> SpanToVector(ReadOnlySpan<byte> span)
        {
            Vector256<byte> v = new Vector256<byte>();
            ref byte vBytes = ref Unsafe.As<Vector256<byte>, byte>(ref v);
            Span<byte> vSpan = MemoryMarshal.CreateSpan(ref vBytes, 32);
            span.Slice(0, Math.Min(32, span.Length)).CopyTo(vSpan);
            return v;
        }
        static int SpanToInt(ReadOnlySpan<byte> span)
        {
            int v = 0;
            ref byte vBytes = ref Unsafe.As<int, byte>(ref v);
            Span<byte> vSpan = MemoryMarshal.CreateSpan(ref vBytes, 4);
            span.Slice(0, Math.Min(4, span.Length)).CopyTo(vSpan);
            return v;
        }

        bool IsMatch(EntryHeader* entry, Vector256<byte> name, int len)
        {
            Vector256<byte> entryName = Vector256.Load((byte*)entry + Unsafe.SizeOf<EntryHeader>());
            return entry->Length == len && name == (entryName & SIMDHelpers.GetLeftMask(len));
        }

        bool IsMatch(EntryHeader* entry, Vector256<byte> name1, Vector256<byte> name2, Vector256<byte> name3, int name4)
        {
            byte* nameStart = (byte*)entry + Unsafe.SizeOf<EntryHeader>();
            Vector256<byte> entryName1 = Vector256.Load(nameStart);
            Vector256<byte> entryName2 = Vector256.Load(nameStart + 32);
            Vector256<byte> entryName3 = Vector256.Load(nameStart + 64);
            int entryName4 = *(int*)(nameStart + 96);
            return name1 == entryName1 && name2 == entryName2 && name3 == entryName3 && name4 == entryName4;
        }

        void InitEntry(EntryHeader* entry, Vector256<byte> name)
        {
            entry->Value.Init();
            entry->Next = uint.MaxValue;
            entry->Length = (ushort)name.IndexOf(0);
            name.CopyTo(new Span<byte>((byte*)entry + Unsafe.SizeOf<EntryHeader>(), 32));
        }

        void InitEntry(EntryHeader* entry, Vector256<byte> name1, Vector256<byte> name2, Vector256<byte> name3, int name4)
        {
            entry->Value.Init();
            entry->Next = uint.MaxValue;
            entry->Length = 100;
            Span<byte> nameBytes = new Span<byte>((byte*)entry + Unsafe.SizeOf<EntryHeader>(), 100);
            name1.CopyTo(nameBytes);
            name2.CopyTo(nameBytes.Slice(32));
            name3.CopyTo(nameBytes.Slice(64));
            *(int*)((byte*)entry + Unsafe.SizeOf<EntryHeader>() + 96) = name4;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int GetHashCode(Vector256<byte> name)
        {
            return GetHashCode(name.AsInt64()[0]);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int GetHashCode(long nameBytes)
        {
            return (int)((nameBytes * 0x353a6569c53a6569) >> 23);
        }
    }
}
