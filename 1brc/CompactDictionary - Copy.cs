using System.Diagnostics;
using System.Drawing;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using System.Text;

namespace _1brc
{
    [StructLayout(LayoutKind.Explicit)]
    public struct Entry11WithOverflow
    {
        [FieldOffset(0)] public Vector128<byte> ShortName;
        [FieldOffset(11)] public int OverflowIndex;
        [FieldOffset(14)] public ushort Value;
    }

    public unsafe interface IEntryOps<TName, TEntry> where TName : struct
    {
        abstract static string GetName(ref TEntry entry, byte[] overflowNameBuffer);

        abstract static ushort GetValue(ref TEntry entry);
        abstract static long GetInitialNameBytes(TName name);
        abstract static long GetInitialNameBytes(ref TEntry entry);
        abstract static bool IsMatch(ref TEntry entry, byte[] overflowNameBuffer, TName name);
        abstract static void InitEntry(ref TEntry entry, TName name, ushort value, Func<int, (int,Memory<byte>)> allocateNameOverflowBytes);
    }

    public unsafe class Index<TName, TEntry, TOps> where TName : struct
                                                   where TEntry : struct
                                                   where TOps : struct, IEntryOps<TName, TEntry> 

                                                   
    {
        const int EntriesCount = 10_003;
        const float MaxLoadFactor = 0.75F;
        TEntry[] _entries;
        ushort _freeEntryIndex;

        byte[] _overflowNameBuffer;
        int _overflowNameBufferFreeOffset;
        
        ushort[] _buckets;
        int _bucketsIndexMask;

        bool _newElementsPresent;
        bool _fourEntryMiss;

        public Index(int initialBucketCapacity, int overflowNameCount)
        {
            _entries = new TEntry[EntriesCount];

            _overflowNameBuffer = new byte[overflowNameCount + 32];
            _overflowNameBufferFreeOffset = 32;

            _buckets = new ushort[initialBucketCapacity];
            for (int i = 0; i < _buckets.Length; i++) _buckets[i] = ushort.MaxValue;
            Debug.Assert(BitOperations.IsPow2(_buckets.Length));
            _bucketsIndexMask = _buckets.Length - 1;

            _overflowNameBuffer = new byte[overflowNameCount];
        }

        public int Count => _freeEntryIndex;

        public IEnumerable<string> GetEntries()
        {
            List<string> entries = new List<string>();
            for (short i = 0; i < _freeEntryIndex;)
            {
                ref TEntry entry = ref _entries[i];
                entries.Add(TOps.GetName(ref entry, _overflowNameBuffer));
            }
            return entries;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetHashCode(long nameBytes)
        {
            return (int)((nameBytes * 0x353a6569c53a6569) >> 23);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ushort GetOrCreate(TName name)
        {
            int hash = GetHashCode(TOps.GetInitialNameBytes(name));
            for(; ; hash++)
            {
                ushort bucketIdx = (ushort)(hash & _bucketsIndexMask);
                ushort entryIdx = _buckets[bucketIdx];
                for (ushort i = 0; i < 4; i++)
                {
                    ushort curEntryIdx = (ushort)(entryIdx + i);
                    if(curEntryIdx >= _freeEntryIndex)
                    {
                        return Create(bucketIdx, name);
                    }

                    ref TEntry entry = ref _entries[curEntryIdx];
                    if (TOps.IsMatch(ref entry, _overflowNameBuffer, name))
                    {
                        return TOps.GetValue(ref entry);
                    }
                }
                _fourEntryMiss = true;
            }
        }

        ushort Create(int bucketIdx, TName name)
        {
            _newElementsPresent = true;
            ushort entryIdx = _freeEntryIndex;
            ref TEntry entry = ref _entries[entryIdx];
            TOps.InitEntry(ref entry, name, _freeEntryIndex, AllocateNameBytes);
            _buckets[bucketIdx] = entryIdx;
            _freeEntryIndex++;
            return entryIdx;
        }

        (int, Memory<byte>) AllocateNameBytes(int size)
        {
            Debug.Assert(_overflowNameBufferFreeOffset + size <= _overflowNameBuffer.Length);
            int nameStart = _overflowNameBufferFreeOffset;
            _overflowNameBufferFreeOffset += size;
            return (nameStart, new Memory<byte>(_overflowNameBuffer, nameStart, size));
        }

        public bool NeedsRehash => _fourEntryMiss && _newElementsPresent ||
            (_buckets.Length * MaxLoadFactor < _freeEntryIndex);

        public Index<TName, TEntry, TOps> Rehash()
        {
            (ushort, ushort)[] entries = new (ushort, ushort)[_freeEntryIndex];
            for(int i = 0; i < _freeEntryIndex;i++)
            {
                ushort hash = (ushort)GetHashCode(TOps.GetInitialNameBytes(ref _entries[i]));
                entries[i] = ((ushort)(hash & _bucketsIndexMask), (ushort)i);
            }
            Array.Sort(entries, (a, b) => a.Item1 - b.Item1);

            Index<TName, TEntry, TOps> newIndex = new Index<TName, TEntry, TOps>(_buckets.Length, _overflowNameBuffer.Length - 32);
            ushort lastBucketIdx = ushort.MaxValue;
            for(int i = 0; i < entries.Length; i++)
            {
                ushort bucketIdx = entries[i].Item1;
                ushort entryIdx = entries[i].Item2;
                if (bucketIdx != lastBucketIdx)
                {
                    newIndex._buckets[bucketIdx] = (ushort)i;
                    lastBucketIdx = bucketIdx;
                }
                newIndex._entries[i] = _entries[entryIdx];
            }
            newIndex._freeEntryIndex = _freeEntryIndex;

            Array.Copy(_overflowNameBuffer, newIndex._overflowNameBuffer, _overflowNameBufferFreeOffset);
            newIndex._overflowNameBufferFreeOffset = _overflowNameBufferFreeOffset;
            return newIndex;
        }
    }

    public struct UtfName32
    {
        public Vector256<byte> NameBytes;
        public int Length;
    }

    struct Entry32
    {
        public Vector256<byte> NameBytes;
        public ushort Value;
    }

    public unsafe struct Index32
    {
        Index<UtfName32, Entry32, EntryOps> _index;

        public Index32()
        {
            _index = new Index<UtfName32, Entry32, EntryOps>(1 << 14, 21 * 10_000);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetOrCreate(UtfName32 name) => _index.GetOrCreate(name);
        public int Count => _index.Count;
        public IEnumerable<string> GetEntries() => _index.GetEntries();
        public bool NeedsRehash => _index.NeedsRehash;
        public void Rehash()
        {
            _index = _index.Rehash();
        }
        struct EntryOps : IEntryOps<UtfName32, Entry32>
        {
            public static long GetInitialNameBytes(UtfName32 name) => name.NameBytes.AsInt64()[0];

            public static long GetInitialNameBytes(ref Entry32 entry) => entry.NameBytes.AsInt64()[0];

            public static string GetName(ref Entry32 entry, byte[] overflowNameBuffer)
            {
                Span<byte> nameBuffer = MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref entry, 1));
                int idx = nameBuffer.IndexOf((byte)0);
                if (idx >= 0)
                {
                    nameBuffer = nameBuffer.Slice(0, idx);
                }
                return Encoding.UTF8.GetString(nameBuffer);
            }

            public static ushort GetValue(ref Entry32 entry) => entry.Value;

            public static void InitEntry(ref Entry32 entry, UtfName32 name, ushort value, Func<int, (int, Memory<byte>)> allocateNameOverflowBytes)
            {
                entry.NameBytes = name.NameBytes;
                entry.Value = value;
            }

            public static bool IsMatch(ref Entry32 entry, byte[] overflowNameBuffer, UtfName32 name) => entry.NameBytes == name.NameBytes;
        }
    }

    public unsafe struct Index11WithOverflow
    {
        Index<UtfName32, Entry11WithOverflow, EntryOps> _index;

        public Index11WithOverflow()
        {
            _index = new Index<UtfName32, Entry11WithOverflow, EntryOps>(1 << 14, 21 * 10_000);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetOrCreate(UtfName32 name) => _index.GetOrCreate(name);
        public int Count => _index.Count;
        public IEnumerable<string> GetEntries() => _index.GetEntries();
        public bool NeedsRehash => _index.NeedsRehash;
        public void Rehash()
        {
            _index = _index.Rehash();
        }
        struct EntryOps : IEntryOps<UtfName32, Entry11WithOverflow>
        {
            public static long GetInitialNameBytes(UtfName32 name) => name.NameBytes.AsInt64()[0];

            public static long GetInitialNameBytes(ref Entry11WithOverflow entry) => entry.ShortName.AsInt64()[0];

            public static string GetName(ref Entry11WithOverflow entry, byte[] overflowNameBuffer)
            {

                Span<byte> nameBuffer = MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref entry, 1));
                int idx = nameBuffer.IndexOf((byte)0);
                if (idx >= 0)
                {
                    nameBuffer = nameBuffer.Slice(0, idx);
                }
                return Encoding.UTF8.GetString(nameBuffer);
            }

            public static ushort GetValue(ref Entry11WithOverflow entry) => entry.Value;

            public static void InitEntry(ref Entry11WithOverflow entry, UtfName32 name, ushort value, Func<int, (int, Memory<byte>)> allocateNameOverflowBytes)
            {
                entry.ShortName = name.NameBytes.GetLower();
                int overflowByteCount = Math.Max(0, name.Length + 1 - 11);
                (int offset, Memory<byte> overflowBytes) = allocateNameOverflowBytes(overflowByteCount);
                entry.OverflowIndex &= ~0xFFFFFF;
                entry.OverflowIndex |= offset-11;
                for (int i = 11; i < name.Length + 1; i++)
                {
                    overflowBytes.Span[i - 11] = name.NameBytes[i];
                }
                entry.Value = value;
            }

            public static bool IsMatch(ref Entry11WithOverflow entry, byte[] overflowNameBuffer, UtfName32 name)
            {
                Vector128<byte> compareShortName = Vector128.Equals<byte>(entry.ShortName, name.NameBytes.GetLower());
                uint compareBits = compareShortName.ExtractMostSignificantBits();
                bool shortNameEqual = (compareBits & 0x7FF) == 0x7FF;
                Vector256<byte> overflow = Vector256.Create<byte>(overflowNameBuffer, entry.OverflowIndex & 0xFFFFFF);
                Vector256<byte> compareLongName = Vector256.Equals<byte>(overflow, name.NameBytes);
                uint compareBitsOverflow = compareLongName.ExtractMostSignificantBits();
                uint mask = (uint)((1UL << (name.Length + 1)) - 1);
                mask &= unchecked((uint)~0x7FF);
                bool overflowEqual = (compareBitsOverflow & mask) == mask;
                return shortNameEqual && overflowEqual;
            }
        }
    }


    public unsafe sealed class CompactDictionary2 : IStationDictionary
    {
        const int Size = 1 << 16;
        uint* _buckets;
        byte* _entries;
        uint _nextEntryOffset;
        uint _entriesSize;

        public CompactDictionary2()
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
