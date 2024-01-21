using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using System.Text;
using System.Xml.Linq;

namespace _1brc
{
    interface IEntry
    {
        string GetName();
        Stats GetValue();
    }

    [StructLayout(LayoutKind.Explicit, Size = 64)]
    unsafe struct SmallEntry : IEntry
    {
        [FieldOffset(0)] public Vector256<byte> ShortName;
        [FieldOffset(32)] public Stats Value;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsMatchingName(Vector256<byte> shortName) =>  ShortName.Equals(shortName);

        public string GetName()
        {
            int len = ShortName.IndexOf(0);
            ref byte shortNameRef = ref Unsafe.As<Vector256<byte>, byte>(ref ShortName);
            return Encoding.UTF8.GetString(MemoryMarshal.CreateReadOnlySpan(ref shortNameRef, len));
        }

        public Stats GetValue() => Value;

        public override string ToString()
        {
            return $"ShortName={GetName()} Value={Value}";
        }
    }

    [StructLayout(LayoutKind.Explicit, Size = 128)]
    unsafe struct LargeEntry : IEntry
    {
        [FieldOffset(0)] public Vector256<byte> Name1;
        [FieldOffset(32)] public Vector256<byte> Name2;
        [FieldOffset(64)] public Vector256<byte> Name3;
        [FieldOffset(96)] public int Name4;
        // unused 4 bytes
        [FieldOffset(104)] public Stats Value;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsMatchingName(Vector256<byte> name1, Vector256<byte> name2, Vector256<byte> name3, int name4)
        {
            Vector256<byte> name1Matches = Avx2.CompareEqual(name1, Name1);
            Vector256<byte> name2Matches = Avx2.CompareEqual(name2, Name2);
            Vector256<byte> name3Matches = Avx2.CompareEqual(name3, Name3);
            Vector256<byte> allMatches = name1Matches & name2Matches & name3Matches;
            uint mask = allMatches.ExtractMostSignificantBits();
            return (mask == 0xFFFFFFFF) && (name4 == Name4);
        }

        public string GetName()
        {
            ref byte nameRef = ref Unsafe.As<Vector256<byte>, byte>(ref Name1);
            ReadOnlySpan<byte> nameBytes = MemoryMarshal.CreateReadOnlySpan(ref nameRef, 100);
            int len = nameBytes.IndexOf((byte)0);
            len = (len == -1) ? 100 : len;
            return Encoding.UTF8.GetString(nameBytes.Slice(0,len));
        }

        public Stats GetValue() => Value;

        public override string ToString()
        {
            return $"ShortName={GetName()} Value={Value}";
        }
    }

    unsafe struct EntryArray<TEntry> where TEntry : unmanaged, IEntry
    {
        List<int> _populatedIndices;
        TEntry* _entries;

        public void Init(int size)
        {
            _populatedIndices = new List<int>();
            int entriesSize = Unsafe.SizeOf<TEntry>() * size;
            _entries = (TEntry*)NativeMemory.AlignedAlloc((nuint)entriesSize, 64);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ref TEntry GetEntry(int index) => ref _entries[index];
        public TEntry* GetEntryPtr(int index) => &_entries[index];
        public void MarkEntryInUse(int index) => _populatedIndices.Add(index);

        public IEnumerable<KeyValuePair<string, Stats>> GetEntries()
        {
            foreach (int index in _populatedIndices)
            {
                IEntry e = GetEntry(index);
                yield return new KeyValuePair<string, Stats>(e.GetName(), e.GetValue());
            }
            yield break;
        }
    }

    public unsafe interface IStationDictionary
    {
        Stats* GetOrCreate(Vector256<byte> name, int len);
        Stats* GetOrCreate(Vector256<byte> name1, Vector256<byte> name2, Vector256<byte> name3, int name4, int len);
        Stats* GetOrCreate(ReadOnlySpan<byte> name);
        IEnumerable<KeyValuePair<string, Stats>> GetEntries();
        int Count { get; }
    }

    public unsafe sealed class SparseDictionary : IStationDictionary
    {
        const int Size = 1 << 18;
        EntryArray<SmallEntry> _smallBuckets;
        EntryArray<LargeEntry> _largeBuckets;

        public SparseDictionary()
        {
            _smallBuckets.Init(Size);
            _largeBuckets.Init(Size);
        }

        public int Count { get; private set; }

        public IEnumerable<KeyValuePair<string, Stats>> GetEntries() => _smallBuckets.GetEntries().Concat(_largeBuckets.GetEntries());

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Stats* GetOrCreate(Vector256<byte> name, int len)
        {
            Debug.Assert(len < 32);
            Debug.Assert(name[len] == 0);
            int hash = GetHashCode(name, len);
            return GetOrCreate(hash, name);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Stats* GetOrCreate(int hash, Vector256<byte> name)
        {
            SmallEntry* likelyEntry = _smallBuckets.GetEntryPtr(hash & (Size - 1));
            if (likelyEntry->IsMatchingName(name))
            {
                return &(likelyEntry->Value);
            }
            else
            {
                return GetOrCreateCold(hash, name);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        Stats* GetOrCreateCold(int hash, Vector256<byte> name)
        {
            while (true)
            {
                int entryIdx = hash & (Size - 1);
                SmallEntry* entry = _smallBuckets.GetEntryPtr(entryIdx);
                if (entry->IsMatchingName(name))
                {
                    return  &(entry->Value);
                }
                if (entry->IsMatchingName(Vector256<byte>.Zero))
                {
                    entry->ShortName = name;
                    entry->Value.Init();
                    _smallBuckets.MarkEntryInUse(entryIdx);
                    Count++;
                    return  &(entry->Value);
                }
                else
                {
                    hash++;
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Stats* GetOrCreate(Vector256<byte> name1, Vector256<byte> name2, Vector256<byte> name3, int name4, int len)
        {
            Debug.Assert(len <= 100);
            Debug.Assert(len >= 32);
            Debug.Assert(len >= 64 || name2[len-32] == 0);
            Debug.Assert(len >= 96 || len < 64 || name3[len-64] == 0);
            Debug.Assert(len == 100 || len < 96 || name4 >> (len-96)*8 == 0);
            int hash = GetHashCode(name1, len);
            LargeEntry* likelyEntry = _largeBuckets.GetEntryPtr(hash & (Size - 1));
            if (likelyEntry->IsMatchingName(name1,name2,name3, name4))
            {
                return &(likelyEntry->Value);
            }
            else
            {
                return GetOrCreateCold(hash, name1, name2, name3, name4);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        Stats* GetOrCreateCold(int hash, Vector256<byte> name1, Vector256<byte> name2, Vector256<byte> name3, int name4)
        {
            while (true)
            {
                int entryIdx = hash & (Size - 1);
                LargeEntry* entry = _largeBuckets.GetEntryPtr(entryIdx);
                if (entry->IsMatchingName(name1, name2, name3, name4))
                {
                    return &(entry->Value);
                }
                if (entry->Name1 == Vector256<byte>.Zero)
                {
                    entry->Name1 = name1;
                    entry->Name2 = name2;
                    entry->Name3 = name3;
                    entry->Name4 = name4;
                    entry->Value.Init();
                    _largeBuckets.MarkEntryInUse(entryIdx);
                    Count++;
                    return &(entry->Value);
                }
                else
                {
                    hash++;
                }
            }
        }

        public Stats* GetOrCreate(ReadOnlySpan<byte> name)
        {
            if (name.Length <= 32)
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int GetHashCode(Vector256<byte> name, int len)
        {
            return GetHashCode(name.AsInt64()[0], len);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int GetHashCode(long nameBytes, int len)
        {
            return ((int)(nameBytes + (nameBytes >> 28)) + len);
        }
    }
}
