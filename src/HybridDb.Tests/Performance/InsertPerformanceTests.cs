using System;
using Shouldly;
using Xunit;

namespace HybridDb.Tests.Performance
{
    public class InsertPerformanceTests : PerformanceTests, IUseFixture<InsertPerformanceTests.Fixture>
    {
        protected DocumentStore store;

        [Fact]
        public void SimpleQueryWithoutMaterialization()
        {
            Time(() => store.Insert(store.Configuration.GetDesignFor<Entity>().Table, Guid.NewGuid(), new byte[] {}, new { Asger = "A", Lars = "B" }))
                .ShouldBeLessThan(40);
        }

        public void SetFixture(Fixture data)
        {
            store = data.Store;
        }

        public class Fixture : IDisposable
        {
            readonly DocumentStore store;

            public DocumentStore Store
            {
                get { return store; }
            }

            public Fixture()
            {
                const string connectionString = "data source=.;Integrated Security=True";
                store = DocumentStore.ForTestingWithTempTables(connectionString);
                store.Document<Entity>();
                store.MigrateSchemaToMatchConfiguration();
            }

            public void Dispose()
            {
                store.Dispose();
            }
        }
    }
}