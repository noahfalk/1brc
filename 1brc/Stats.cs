using System.Runtime.InteropServices;

namespace _1brc
{
    [StructLayout(LayoutKind.Explicit, Size = 16)]
    public struct Stats
    {
        [FieldOffset(0)] long _sum;
        [FieldOffset(8)] short _min;
        [FieldOffset(10)] short _max;
        [FieldOffset(12)] int _count;

        public void Init()
        {
            _sum = 0;
            _min = short.MaxValue;
            _max = short.MinValue;
            _count = 0;
        }

        public void InsertMeasurement(short fixedPointValue)
        {
            _sum += fixedPointValue;
            if (fixedPointValue < _min) { _min = fixedPointValue; }
            if (fixedPointValue > _max) { _max = fixedPointValue; }
            _count++;
        }

        public void Merge(Stats other)
        {
            _sum += other._sum;
            _min = Math.Min(_min, other._min);
            _max = Math.Max(_max, other._max);
            _count += other._count;
        }

        public int Count => _count;

        public override string ToString() => $"{_min/10.0F:N1}/{(double)_sum/_count/10.0F:N1}/{_max/10.0F:N1}";
    }
}