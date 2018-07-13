using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Shouldly;
using Xunit;
using static HybridDb.Helpers;

namespace HybridDb.Tests
{
    public class DocumentStoreTests
    {
        readonly DocumentStore store;

        public DocumentStoreTests()
        {
            store = new DocumentStore("Server=.;Integrated Security=True");

            store.Up();
        }

        [Fact]
        public async Task InsertGet()
        {
            await store.Execute(ListOf(new Insert("a", new Dictionary<string, string[]>(), "{}")));

            var result = await store.Get("a");

            result.Key.ShouldBe("{}");
            result.Document.ShouldBe("{}");
        }
    }
}
