using MongoDB.Driver;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace MongoAsyncTest
{
    public static class AsyncCursorExtensions
    {
        public static async IAsyncEnumerable<T> ToAsyncEnumerable<T>(this IAsyncCursor<T> cursor)
        {
            while (await cursor.MoveNextAsync())
            {
                foreach (var d in cursor.Current)
                    yield return d;
            }
        }
    }

    public class TestDocumentCollection
    {
        const string CollectionName = "TestDocuments";
        private readonly MongoUrl _url;

        public TestDocumentCollection(string connectionString)
        {
            _url = MongoUrl.Create(connectionString);
        }

        private IMongoDatabase GetDatabase()
        {
            var clientSettings = MongoClientSettings.FromUrl(_url);
            var client = new MongoClient(clientSettings);
            return client.GetDatabase(_url.DatabaseName);
        }

        private IMongoCollection<TestDocument> GetCollection()
        {
            return GetDatabase().GetCollection<TestDocument>(CollectionName);
        }

        public async Task InsertManyAsync(IEnumerable<TestDocument> documents)
        {
            var collection = GetCollection();
            await collection.InsertManyAsync(documents);
        }

        public async Task<IList<TestDocument>> GetDocumentsAsList()
        {
            var collection = GetCollection();
            return await collection.Find(Builders<TestDocument>.Filter.Empty).ToListAsync();
        }

        public async Task<IAsyncEnumerable<TestDocument>> GetDocumentsAsync()
        {
            var collection = GetCollection();
            var cursor = await collection.Find(Builders<TestDocument>.Filter.Empty).ToCursorAsync();
            return cursor.ToAsyncEnumerable();
        }
    }
}
