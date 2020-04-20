using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using MongoDB.Driver.GridFS;
using System;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;

namespace FileApi.Storage
{
    public class FileStorage
    {
        private enum FileState
        {
            Uploading,
            Uploaded
        }

        private class FileDocument
        {
            public int Id { get; set; }
            public string Consumer { get; set; }
            public string FileName { get; set; }
            [BsonRepresentation(BsonType.String)]
            public FileState State { get; set; }
        }

        const string CollectionName = "TestDocuments";
        private readonly MongoUrl _url;

        public FileStorage(string connectionString)
        {
            _url = MongoUrl.Create(connectionString);
        }

        private IMongoClient GetClient()
        {
            var clientSettings = MongoClientSettings.FromUrl(_url);
            return new MongoClient(clientSettings);
        }

        public async Task SaveFile(int id, string consumer, Stream file)
        {
            var client = GetClient();

            using (var session = await client.StartSessionAsync())
            {
                session.StartTransaction();

                var fileDoc = new FileDocument
                {
                    Id = id,
                    Consumer = consumer,
                    FileName = $"{id}_{consumer}",
                    State = FileState.Uploaded
                };

                var bucket = new GridFSBucket(client.GetDatabase(_url.DatabaseName), new GridFSBucketOptions { BucketName = "Uploads" });
                await bucket.UploadFromStreamAsync(fileDoc.FileName, file);

                var collection = client.GetDatabase(_url.DatabaseName).GetCollection<FileDocument>(CollectionName);
                await collection.InsertOneAsync(fileDoc);

                await session.CommitTransactionAsync();
            }
        }

        public async Task SaveFileZip(int id, string consumer, Stream file)
        {
            var client = GetClient();

            using (var session = await client.StartSessionAsync())
            {
                session.StartTransaction();

                var fileDoc = new FileDocument
                {
                    Id = id,
                    Consumer = consumer,
                    FileName = $"{id}_{consumer}",
                    State = FileState.Uploaded
                };

                var bucket = new GridFSBucket(client.GetDatabase(_url.DatabaseName), new GridFSBucketOptions { BucketName = "Uploads" });
                using (var uploadStream = await bucket.OpenUploadStreamAsync(fileDoc.FileName))
                using (var gzipStream = new GZipStream(uploadStream, CompressionLevel.Optimal))
                {
                    await file.CopyToAsync(gzipStream);
                }

                var collection = client.GetDatabase(_url.DatabaseName).GetCollection<FileDocument>(CollectionName);
                await collection.InsertOneAsync(fileDoc);

                await session.CommitTransactionAsync();
            }
        }

        public async Task ReadFile(int id, string consumer, Stream stream)
        {
            var client = GetClient();

            using (var session = await client.StartSessionAsync())
            {
                var collection = client.GetDatabase(_url.DatabaseName).GetCollection<FileDocument>(CollectionName);
                var cursor = await collection.FindAsync(Builders<FileDocument>.Filter.And(
                    Builders<FileDocument>.Filter.Eq(x => x.Id, id),
                    Builders<FileDocument>.Filter.Eq(x => x.Consumer, consumer)));
                var filedoc = await cursor.SingleAsync();

                var bucket = new GridFSBucket(client.GetDatabase(_url.DatabaseName), new GridFSBucketOptions { BucketName = "Uploads" });
                await bucket.DownloadToStreamAsync(filedoc.FileName, stream);
            }
        }

        public async Task ReadFileZip(int id, string consumer, Stream stream)
        {
            var client = GetClient();

            using (var session = await client.StartSessionAsync())
            {
                var collection = client.GetDatabase(_url.DatabaseName).GetCollection<FileDocument>(CollectionName);
                var cursor = await collection.FindAsync(Builders<FileDocument>.Filter.And(
                    Builders<FileDocument>.Filter.Eq(x => x.Id, id),
                    Builders<FileDocument>.Filter.Eq(x => x.Consumer, consumer)));
                var filedoc = await cursor.SingleAsync();

                var bucket = new GridFSBucket(client.GetDatabase(_url.DatabaseName), new GridFSBucketOptions { BucketName = "Uploads", ChunkSizeBytes = 1_048_576 });
                using (var downloadStream = await bucket.OpenDownloadStreamByNameAsync(filedoc.FileName))
                using (var gzipStream = new GZipStream(downloadStream, CompressionMode.Decompress))
                {
                    await gzipStream.CopyToAsync(stream);
                }
            }
        }

        public async Task UploadFileZip(int id, string consumer, Func<Stream, Task> writer)
        {
            var client = GetClient();
            using (var session = await client.StartSessionAsync())
            {
                var fileDoc = new FileDocument
                {
                    Id = id,
                    Consumer = consumer,
                    FileName = $"{id}_{consumer}",
                    State = FileState.Uploading
                };

                var collection = client.GetDatabase(_url.DatabaseName).GetCollection<FileDocument>(CollectionName);
                await collection.InsertOneAsync(fileDoc);

                var bucket = new GridFSBucket(client.GetDatabase(_url.DatabaseName), new GridFSBucketOptions { BucketName = "Uploads" });
                using (var uploadStream = await bucket.OpenUploadStreamAsync(fileDoc.FileName))
                using (var gzipStream = new GZipStream(uploadStream, CompressionLevel.Optimal))
                {
                    await writer(gzipStream);
                }

                await collection.UpdateOneAsync(
                    Builders<FileDocument>.Filter.And(
                        Builders<FileDocument>.Filter.Eq(x => x.Id, id),
                        Builders<FileDocument>.Filter.Eq(x => x.Consumer, consumer)),
                    Builders<FileDocument>.Update.Set(x => x.State, FileState.Uploaded));
            }
        }
    }
}
