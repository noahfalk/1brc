using _1brc;
using System.Runtime.CompilerServices;
using Xunit;

namespace Tests
{
    public class FullRunTests
    {
        [Theory]
        [MemberData(nameof(MeasurementFilesAndResults))]
        public static void VerifyMemoryMapped(string inputFile, string expectedResultFile)
        {
            string resultText = Brc.ProcessFile(new MemoryMappedIO(inputFile));
            string expectedResults = File.ReadAllText(expectedResultFile);
            Assert.Equal(expectedResults, resultText);
        }

        [Theory]
        [MemberData(nameof(MeasurementFilesAndResults))]
        public static void VerifyRandomAccess(string inputFile, string expectedResultFile)
        {
            string resultText = Brc.ProcessFile(new RandomAccessIO(inputFile));
            string expectedResults = File.ReadAllText(expectedResultFile);
            Assert.Equal(expectedResults, resultText);
        }

        static int CountRows(Dictionary<string, Stats> dictionary) => dictionary.Values.Sum(v => v.Count);

        public static TheoryData<string, string> MeasurementFilesAndResults
        {
            get
            {
                var data = new TheoryData<string, string>();
                string brcDir = Path.Combine(GetSourceDir(), @"../../1brc_data/");
                if (Directory.Exists(brcDir))
                {
                    foreach (string inputFile in Directory.GetFiles(brcDir, "measurements*.txt"))
                    {
                        string fullInputFile = Path.GetFullPath(inputFile);
                        string resultFile = Path.ChangeExtension(fullInputFile, ".out");
                        if (!File.Exists(resultFile))
                        {
                            throw new Exception($"Expected to find result file {resultFile} for corresponding input file {fullInputFile}");
                        }
                        data.Add(fullInputFile, resultFile);
                    }
                }
                if(!data.Any())
                {
                    throw new Exception("Expected to find measurement*.txt files in " + brcDir);
                }
                return data;
            }
        }

        static string GetSourceDir([CallerFilePath] string path = "") => Path.GetDirectoryName(path)!;
    }
}
