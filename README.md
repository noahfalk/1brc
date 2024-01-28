# (Unofficial) 1BRC entry

I've been having a blast poking around at [1BRC](https://github.com/gunnarmorling/1brc), thanks [Gunnar Morling](https://www.morling.dev/blog/one-billion-row-challenge/)! I couldn't help but go down the rabbit hole :)

I appreciate all the other participants who shared their solutions. Many of the optimizations here were inspired by what others discovered
and then I pushed it a little farther. For the moment on Jan 21 2024 this might be the fastest known implementation on the Hexner CCX33 hardware but I hope this is a useful stepping stone for others on the way to even higher perf.

In order to do benchmarking I grabbed access to the Hexner CCX33 (8 cores) hoping to get some numbers that are comparable to the official Java leaderboard 
that Gunnar is running. Gunnar is far more diligent than I am however so I just ran a few benchmarks against some of the other really speedy
unofficial implementations that I was aware of to establish some frame of reference.

| Implementation                                     |   Default data             | 10-K variation    |
| :--------------------------------------------------| :------------------------- | :---------------- |
| This Repo                                          |  1.381                     |  3.739            |
| [lehuyduc](https://github.com/lehuyduc/1brc-simd)  |  1.897                     |  3.858            |
| [buybackoff](https://github.com/buybackoff/1brc) (Added 1/22)  |  2.356                     |  4.751            |
| [nietras](https://github.com/nietras/1brc.cs)      |  2.971                     |  4.564            |

## Is it really that fast?


**Update 1/22**:
@buybackoff kindly added this entry to his [cross-language 1BRC results](https://hotforknowledge.com/2024/01/13/1brc-in-dotnet-among-fastest-on-linux-my-optimization-journey/#results) so now I feel better that others were able to reproduce it. Unlike my data he
actually keeps his up-to-date :) At the moment on his machine @lehuyduc's updated entry is the leader on the 10-K variation and this one is the leader on the default data.
A little more benchmarking on some of the top Java entries suggests my CCX33 instance posted times 5-10% better than the same entries on Gunnar's machine. @lehuyduc also pointed out that Gunnar moved to using the Hexner AX161 instead and that the input data generators aren't deterministic so it remains challenging to compare results across different benchmarking environments without a good amount of fudge factor.
**End Updates**

At the moment the top entries on [Gunnar's official leaderboard](https://github.com/gunnarmorling/1brc/tree/main?tab=readme-ov-file#results) are clocking in around ~2.5s for the default data on similar hardware so this approach is either a substantial improvement or something fishy is going on. I'm hoping at least a few other folks will run this solution and sanity check that this is a reproducible result before I put too much credence in it. As far as I know this solution is not using any [Yolo Optimizations](https://hotforknowledge.com/2024/01/13/1brc-in-dotnet-among-fastest-on-linux-my-optimization-journey/). It may not be performant on pathological input but it is intended to be always correct.

## Running the code

After cloning the repo this is what I do to run the code and measure performance (assuming you have already created the measurements.txt file):
```
cd noahfalk_1brc/1brc/
dotnet publish -c release -r linux-x64
cd bin/release/net8.0/linux-x64/publish/
hyperfine -w 1 -r 5 -- "./1brc ~/git/1brc_data/measurements.txt"
```

## What is different from other implementations?

Many of the optimizations other top entries use are employed here too: custom parsers, custom dictionaries, SIMD, and multi-threading. Ragnar Groot Koerkamp covers a ton of this in [his great post](https://curiouscoding.nl/posts/1brc/). Using that as a baseline here are some changes I did that didn't seem so common:

### Parsing 4 temperatures with SIMD

There is a lot of impressive bit manipulation in other entries that parses one UTF8 temperature, but if we are parsing multiple rows at the same time we can use SIMD to parse several temperatures at the same time. After locating the semicolons the code loads the next 8 bytes * 4 rows into a 32 byte SIMD register and parses the whole thing at once. I think it was a little over 20 instructions to parse 4 of them, as well as calculating the distance between the semicolons and newlines. 

### Doing other row bookkeeping with SIMD

Once we are producing temperature text length as a 4 wide SIMD register we can do more. We can track sets of four pointers in SIMD registers for row cursors and loop bounds. This lets us update them and compare them with fewer instructions. Not everything was profitable though as there are costs to move data back and forth between general purpose and SIMD registers. In particular I thought a SIMD quadruple row hash might be interesting but my hash function wasn't complex enough to make it worthwhile loading and unloading the register. For a more complicated hash function it certainly might be. 

### Parsing 8 rows in parallel per thread

If 4 rows in parallel is good, are 8 even better? Empirically on my dev machine, yes, although not by much. I assume it offers yet more opportunities for instruction level parallelism and especially it lets us get more loads from memory running in parallel. The input data loads are fairly predictable for the hardware prefetcher but the dictionary entry loads are far more randomized.

### File reading vs. Memory Mapping

@nietras drew my attention to this and showed that sometimes file reading performed better. Oddly my experiments showed that file reading performed better even on Linux whereas he came to the opposite conclusion. As best I can tell the memory read throughput is a bit faster with memory mapping, but it wasn't enough to outweigh some fixed costs of creating and releasing the mapping. There are likely many uncontrolled variables that differ between our experiments leading to different results.

### Optimizing a loop for the default data

This code has a dedicated loop that handles data where all the station names are less than 32 bytes. If a name is longer than that it detects it, breaks out of the loop, and uses a separate slower loop from that point onwards. 

### Different custom dictionaries

Not only are the loops different, it also gives us an opportunity to switch between two different custom dictionary implementations. The default data gives us a small number of short names so size of the dictionary isn't large and we can get away with very space inefficient hashtable choices to maximize lookup performance. The 10K challenge on the other hand is completely different - this one depends heavily on cache locality. When I first ran it with the SparseDictionary my times were abysmal and I got around a 6x speedup making a new custom dictionary that was more memory efficient. I think continued improvement on the dictionary memory consumption is a substantial opportunity in the 10K challenge, I didn't spend that much time on it relative to default data challenge.

### Prefetching

In an attempt to improve the dictionary lookup performance I re-ordered the code a bit. First we calculate the likely entry that will have our station data, then issue a prefetch for it, then go off to do some other compute heavy parsing and finally return to load the entry. CPU pipelining surely was already helping us, but making the load visible a little earlier did appear to offer some improvement.


## Go out and enjoy the 1BRC

I hope you are enjoying this as much as I did and I am looking forward to even faster entries that folks discover. Performance optimization is a very fun rabbit hole!