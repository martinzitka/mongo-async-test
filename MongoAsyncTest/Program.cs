using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace MongoAsyncTest
{
    class Program
    {
        private class StopwatchScope: IDisposable
        {
            private readonly Stopwatch _sw;
            private readonly Action<Stopwatch> _action;

            public StopwatchScope(Action<Stopwatch> action)
            {
                _action = action;
                _sw = new Stopwatch();
                _sw.Start();
            }

            public void Dispose()
            {
                _sw.Stop();
                _action(_sw);
            }
        }

        private static IEnumerable<TestDocument> GenerateRandomDocuments(int count)
        {
            var rnd = new Random(0);
            var tdg = new TestDocumentGenerator(rnd);

            for (var i = 0; i < count; i++)
            {
                if (i % 100_000 == 0)
                    Console.WriteLine($"{i}");
                yield return tdg.GetNext();
            }
        }

        private static async Task InsertRandomDocuments(TestDocumentCollection collection)
        {
            await collection.InsertManyAsync(GenerateRandomDocuments(1_000_000).ToList());
        }

        private static int GetDocumentCount(IEnumerable<TestDocument> documents)
        {
            var i = 0;
            foreach (var doc in documents)
                i++;

            return i;
        }

        private static async Task<int> GetDocumentCountAsync(IAsyncEnumerable<TestDocument> documents)
        {
            var i = 0;
            await foreach (var doc in documents)
                i++;

            return i;
        }

        static async Task Main(string[] args)
        {
            var cs = "mongodb://localhost/PerformanceTest";
            var collection = new TestDocumentCollection(cs);

            //await InsertRandomDocuments(collection);

            using (new StopwatchScope(sw => Console.WriteLine($"Sync list duration: {sw.ElapsedMilliseconds} ms")))
            {
                var count = GetDocumentCount(await collection.GetDocumentsAsList());
                var mem = GC.GetTotalMemory(true);
                Console.WriteLine($"Sync list count: {count}, memory: {Process.GetCurrentProcess().WorkingSet64 / 1_048_576} MB, {mem / 1_048_576} MB");
            }

            using (new StopwatchScope(sw => Console.WriteLine($"Async list duration: {sw.ElapsedMilliseconds} ms")))
            {
                var count = await GetDocumentCountAsync(await collection.GetDocumentsAsync());
                var mem = GC.GetTotalMemory(true);
                Console.WriteLine($"Async list count: {count}, memory: {Process.GetCurrentProcess().WorkingSet64 / 1_048_576} MB, {mem / 1_048_576} MB");
            }
        }
    }
}
