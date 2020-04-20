using FileApi.Storage;
using Newtonsoft.Json;
using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GridFsTest
{
    public class Program
    {
        private class TestDataGenerator
        {
            private readonly Random _rnd;

            public TestDataGenerator(Random rnd)
            {
                _rnd = rnd;
            }

            private string GetRandomString(int length)
            {
                var chars = 'z' - '0' + 1;
                var builder = new StringBuilder();

                for (var i = 0; i < length; i++)
                {
                    var r = (char)_rnd.Next(chars);
                    var position = (char)('0' + r);
                    builder.Append(position);
                }


                return builder.ToString();
            }

            public Inner GetRandomInner()
            {
                return new Inner
                {
                    Info = GetRandomString(50),
                    Number = _rnd.Next()
                };
            }

            public TestData GetNext()
            {
                var innerInfos =
                    Enumerable.Repeat(0, _rnd.Next(20))
                        .Select(x => GetRandomInner())
                        .ToArray();

                return new TestData
                {
                    Id = GetRandomString(20),
                    Name = GetRandomString(10),
                    InnerInfos = innerInfos
                };
            }
        }

        private class Inner
        {
            public string Info { get; set; }
            public int Number { get; set; }
        }

        private class TestData
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public Inner[] InnerInfos { get; set; }
        }

        static async Task Main(string[] args)
        {
            const string cs = "mongodb://localhost/GridFsTest";
            var storage = new FileStorage(cs);

            var generator = new TestDataGenerator(new Random(0));
            var serializer = JsonSerializer.Create();

            await storage.UploadFileZip(1, "test", stream =>
            {
                using (var writer = new StreamWriter(stream))
                using (var jsonTextWriter = new JsonTextWriter(writer))
                {
                    for (int i = 0; i < 100_000; i++)
                    {
                        if (i % 1000 == 0)
                            Console.WriteLine($"{i}");
                        var data = generator.GetNext();
                        serializer.Serialize(jsonTextWriter, data);
                    }
                }
                return Task.CompletedTask;
            });
        }
    }
}
