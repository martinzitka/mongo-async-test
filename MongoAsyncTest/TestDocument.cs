using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MongoAsyncTest
{
    public class Inner
    {
        [BsonElement]
        public string Info { get; set; }

        [BsonElement]
        public int Number { get; set; }
    }

    public class TestDocument
    {
        [BsonId]
        public string Id { get; set; }

        [BsonElement]
        public string Name { get; set; }

        [BsonElement]
        public Inner[] InnerInfos { get; set; }
    }

    public class TestDocumentGenerator
    {
        private readonly Random _rnd;

        public TestDocumentGenerator(Random rnd)
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

        public TestDocument GetNext()
        {
            var innerInfos =
                Enumerable.Repeat(0, _rnd.Next(20))
                    .Select(x => GetRandomInner())
                    .ToArray();

            return new TestDocument
            {
                Id = GetRandomString(20),
                Name = GetRandomString(10),
                InnerInfos = innerInfos
            };
        }
    }
}
