﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Transactions;
using HybridDb.Commands;
using HybridDb.Config;
using HybridDb.Migrations;
using Shouldly;
using Xunit;
using Xunit.Extensions;

namespace HybridDb.Tests
{
    public class DocumentStoreTests : HybridDbStoreTests
    {
        readonly byte[] documentAsByteArray;

        public DocumentStoreTests()
        {
            documentAsByteArray = new[] {(byte) 'a', (byte) 's', (byte) 'g', (byte) 'e', (byte) 'r'};
        }

        [Fact]
        public void CanInsert()
        {
            Document<Entity>().With(x => x.Field);

            var id = Guid.NewGuid();
            var table = store.Configuration.GetDesignFor<Entity>();
            store.Insert(table.Table, id, new {Field = "Asger", Document = documentAsByteArray});

            var row = database.RawQuery<dynamic>("select * from #Entities").Single();
            ((Guid) row.Id).ShouldBe(id);
            ((Guid) row.Etag).ShouldNotBe(Guid.Empty);
            Encoding.ASCII.GetString((byte[]) row.Document).ShouldBe("asger");
            ((string) row.Field).ShouldBe("Asger");
        }

        [Fact]
        public void CanInsertDynamically()
        {
            Document<Entity>().With(x => x.Field);
            
            var id = Guid.NewGuid();
            store.Insert(new DynamicDocumentTable("Entities"), id, new { Field = "Asger", Document = documentAsByteArray });

            var row = database.RawQuery<dynamic>("select * from #Entities").Single();
            ((Guid) row.Id).ShouldBe(id);
            ((Guid) row.Etag).ShouldNotBe(Guid.Empty);
            Encoding.ASCII.GetString((byte[]) row.Document).ShouldBe("asger");
            ((string) row.Field).ShouldBe("Asger");
        }

        [Fact(Skip = "We will maybe not support this in the future. Just get the table from QuerySchema and use that, when it can return DocumentTable and not just Table.")]
        public void CanInsertNullsDynamically()
        {
            Document<Entity>().With(x => x.Field);

            store.Insert(new DynamicDocumentTable("Entities"),
                         Guid.NewGuid(),
                         new Dictionary<string, object> {{"Field", null}});

            var row = database.RawQuery<dynamic>("select * from #Entities").Single();
            ((string) row.Field).ShouldBe(null);
        }

        [Fact(Skip = "This will fail on first insert now, but we might want to check it at configuration time, but only if other stores do not support either.")]
        public void FailsOnSettingUpComplexProjections()
        {
            Should.Throw<ArgumentException>(() =>
            {
                Document<Entity>().With(x => x.Complex);
            });
        }

        [Fact]
        public void FailsOnDynamicallyInsertedComplexProjections()
        {
            Document<Entity>();
            
            Should.Throw<ArgumentException>(() =>
                store.Insert(new DynamicDocumentTable("Entities"), Guid.NewGuid(), new { Complex = new Entity.ComplexType() }));
        }

        [Fact(Skip = "Feature on hold")]
        public void CanInsertCollectionProjections()
        {
            Document<Entity>().With(x => x.Children.Select(y => y.NestedProperty));
            
            var id = Guid.NewGuid();
            var schema = store.Configuration.GetDesignFor<Entity>();
            store.Insert(
                schema.Table, id,
                new
                {
                    Children = new[]
                    {
                        new {NestedProperty = "A"},
                        new {NestedProperty = "B"}
                    }
                });

            var mainrow = database.RawQuery<dynamic>("select * from #Entities").Single();
            ((Guid)mainrow.Id).ShouldBe(id);

            var utilrows = database.RawQuery<dynamic>("select * from #Entities_Children").ToList();
            utilrows.Count.ShouldBe(2);
            
            var utilrow = utilrows.First();
            ((Guid)utilrow.DocumentId).ShouldBe(id);
            ((string)utilrow.NestedString).ShouldBe("A");
        }

        [Fact]
        public void CanUpdate()
        {
            Document<Entity>().With(x => x.Field);
            
            var id = Guid.NewGuid();
            var table = store.Configuration.GetDesignFor<Entity>();
            var etag = store.Insert(table.Table, id, new {Field = "Asger"});

            store.Update(table.Table, id, etag, new {Field = "Lars"});

            var row = database.RawQuery<dynamic>("select * from #Entities").Single();
            ((Guid) row.Etag).ShouldNotBe(etag);
            ((string) row.Field).ShouldBe("Lars");
        }

        [Fact(Skip ="We will maybe not support this in the future. Just get the table from QuerySchema and use that, when it can return DocumentTable and not just Table.")]
        public void CanUpdateDynamically()
        {
            Document<Entity>().With(x => x.Field).With(x => x.Property);
            
            var id = Guid.NewGuid();
            var table = store.Configuration.GetDesignFor<Entity>();
            var etag = store.Insert(table.Table, id, new {Field = "Asger"});

            // Maybe it should not be required to be a DocumentTable. If we do that everything should part of the projection. 
            // If we do not do that, why do we have document as part of the projection? Either or.
            store.Update(new DynamicDocumentTable("Entities"), id, etag, new Dictionary<string, object> { { "Field", null }, { "Property", "Lars" } });

            var row = database.RawQuery<dynamic>("select * from #Entities").Single();
            ((Guid) row.Etag).ShouldNotBe(etag);
            ((string) row.Field).ShouldBe(null);
            ((string) row.Property).ShouldBe("Lars");
        }

        [Fact]
        public void CanUpdatePessimistically()
        {
            Document<Entity>().With(x => x.Field);
            
            var id = Guid.NewGuid();
            var table = store.Configuration.GetDesignFor<Entity>();
            store.Insert(table.Table, id, new {Field = "Asger", Document = new[] {(byte) 'a', (byte) 's', (byte) 'g', (byte) 'e', (byte) 'r'}});

            Should.NotThrow(() => store.Update(table.Table, id, Guid.NewGuid(), new {Field = "Lars"}, lastWriteWins: true));
        }

        [Fact]
        public void UpdateFailsWhenEtagNotMatch()
        {
            Document<Entity>().With(x => x.Field);
                        
            var id = Guid.NewGuid();
            var table = store.Configuration.GetDesignFor<Entity>();
            store.Insert(table.Table, id, new { Field = "Asger", Document = documentAsByteArray });

            Should.Throw<ConcurrencyException>(() => store.Update(table.Table, id, Guid.NewGuid(), new {Field = "Lars"}));
        }

        [Fact]
        public void UpdateFailsWhenIdNotMatchAkaObjectDeleted()
        {
            Document<Entity>().With(x => x.Field);
            
            var id = Guid.NewGuid();
            var etag = Guid.NewGuid();
            var table = store.Configuration.GetDesignFor<Entity>();
            store.Insert(table.Table, id, new { Field = "Asger", Document = documentAsByteArray });

            Should.Throw<ConcurrencyException>(() => store.Update(table.Table, Guid.NewGuid(), etag, new {Field = "Lars"}));
        }

        [Fact]
        public void CanGet()
        {
            Document<Entity>().With(x => x.Field).With(x => x.Complex.ToString());
            
            var id = Guid.NewGuid();
            var table = store.Configuration.GetDesignFor<Entity>();
            var etag = store.Insert(table.Table, id, new {Field = "Asger", ComplexToString = "AB", Document = documentAsByteArray});

            var row = store.Get(table.Table, id);
            row[table.Table.IdColumn].ShouldBe(id);
            row[table.Table.EtagColumn].ShouldBe(etag);
            row[table.Table.DocumentColumn].ShouldBe(documentAsByteArray);
            row[table.Table["Field"]].ShouldBe("Asger");
            row[table.Table["ComplexToString"]].ShouldBe("AB");
        }

        [Fact]
        public void CanGetDynamically()
        {
            Document<Entity>().With(x => x.Field);
            
            var id = Guid.NewGuid();
            var table = store.Configuration.GetDesignFor<Entity>();
            var etag = store.Insert(table.Table, id, new { Field = "Asger", Document = documentAsByteArray });

            var row = store.Get(new DynamicDocumentTable("Entities"), id);
            row[table.Table.IdColumn].ShouldBe(id);
            row[table.Table.EtagColumn].ShouldBe(etag);
            row[table.Table.DocumentColumn].ShouldBe(documentAsByteArray);
            row[table.Table["Field"]].ShouldBe("Asger");
        }

        [Fact]
        public void CanQueryProjectToNestedProperty()
        {
            Document<Entity>().With(x => x.TheChild.NestedDouble);
            
            var id1 = Guid.NewGuid();
            var table = store.Configuration.GetDesignFor<Entity>();
            store.Insert(table.Table, id1, new { TheChildNestedDouble = 9.8d });

            QueryStats stats;
            var rows = store.Query<ProjectionWithNestedProperty>(table.Table, out stats).ToList();

            rows.Single().TheChildNestedDouble.ShouldBe(9.8d);
        }

        [Fact]
        public void CanQueryAndReturnFullDocuments()
        {
            Document<Entity>().With(x => x.Field);
            
            var id1 = Guid.NewGuid();
            var id2 = Guid.NewGuid();
            var id3 = Guid.NewGuid();
            var table = store.Configuration.GetDesignFor<Entity>();
            var etag1 = store.Insert(table.Table, id1, new { Field = "Asger", Document = documentAsByteArray });
            var etag2 = store.Insert(table.Table, id2, new { Field = "Hans", Document = documentAsByteArray });
            store.Insert(table.Table, id3, new { Field = "Bjarne", Document = documentAsByteArray });

            QueryStats stats;
            var rows = store.Query(table.Table, out stats, where: "Field != @name", parameters: new { name = "Bjarne" }).ToList();

            rows.Count().ShouldBe(2);
            var first = rows.Single(x => (Guid)x[table.Table.IdColumn] == id1);
            first[table.Table.EtagColumn].ShouldBe(etag1);
            first[table.Table.DocumentColumn].ShouldBe(documentAsByteArray);
            first[table.Table["Field"]].ShouldBe("Asger");

            var second = rows.Single(x => (Guid)x[table.Table.IdColumn] == id2);
            second[table.Table.IdColumn].ShouldBe(id2);
            second[table.Table.EtagColumn].ShouldBe(etag2);
            second[table.Table.DocumentColumn].ShouldBe(documentAsByteArray);
            second[table.Table["Field"]].ShouldBe("Hans");
        }

        [Fact]
        public void CanQueryAndReturnAnonymousProjections()
        {
            Document<Entity>().With(x => x.Field);
            
            var id = Guid.NewGuid();
            var table = store.Configuration.GetDesignFor<Entity>();

            store.Insert(table.Table, id, new { Field = "Asger", Document = documentAsByteArray });

            var t = new {Field = ""};

            QueryStats stats = null;
            var methodInfo = (from method in store.GetType().GetMethods()
                              where method.Name == "Query" && method.IsGenericMethod
                              select method).Single().MakeGenericMethod(t.GetType());

            var rows = (IEnumerable<dynamic>) methodInfo.Invoke(store, new object[] {table.Table, stats, false, null, "Field = @name", null, "", new {name = "Asger"}});

            rows.Count().ShouldBe(1);
            Assert.Equal("Asger", rows.Single().Field);
        }

        [Fact(Skip = "I believe this is issue #24")]
        public void CanQueryAndReturnValueProjections()
        {
            Document<Entity>().With(x => x.Field);
            
            var id = Guid.NewGuid();
            var table = store.Configuration.GetDesignFor<Entity>();

            store.Insert(table.Table, id, new { Field = "Asger", Document = documentAsByteArray });

            QueryStats stats;
            var rows = store.Query<string>(table.Table, out stats, select: "Field").ToList();

            Assert.Equal("Asger", rows.Single());
        }

        [Fact]
        public void CanQueryDynamicTable()
        {
            Document<Entity>().With(x => x.Field).With(x => x.Property);
            
            var id1 = Guid.NewGuid();
            var id2 = Guid.NewGuid();
            var table = store.Configuration.GetDesignFor<Entity>();
            store.Insert(table.Table, id1, new { Field = "Asger", Property = "A", Document = documentAsByteArray });
            store.Insert(table.Table, id2, new { Field = "Hans", Property = "B", Document = documentAsByteArray });

            QueryStats stats;
            var rows = store.Query(new DynamicDocumentTable("Entities"), out stats, where: "Field = @name", parameters: new { name = "Asger" }).ToList();

            rows.Count().ShouldBe(1);
            var row = rows.Single();
            row[table.Table["Field"]].ShouldBe("Asger");
            row[table.Table["Property"]].ShouldBe("A");
        }

        [Fact]
        public void CanDelete()
        {
            Document<Entity>();
            
            var id = Guid.NewGuid();
            var table = store.Configuration.GetDesignFor<Entity>();
            var etag = store.Insert(table.Table, id, new { });

            store.Delete(table.Table, id, etag);

            database.RawQuery<dynamic>("select * from #Entities").Count().ShouldBe(0);
        }

        [Fact]
        public void CanDeletePessimistically()
        {
            Document<Entity>();
            
            var id = Guid.NewGuid();
            var table = store.Configuration.GetDesignFor<Entity>();
            store.Insert(table.Table, id, new { });

            Should.NotThrow(() => store.Delete(table.Table, id, Guid.NewGuid(), lastWriteWins: true));
        }

        [Fact]
        public void DeleteFailsWhenEtagNotMatch()
        {
            Document<Entity>();
            
            var id = Guid.NewGuid();
            var table = store.Configuration.GetDesignFor<Entity>();
            store.Insert(table.Table, id, new { });

            Should.Throw<ConcurrencyException>(() => store.Delete(table.Table, id, Guid.NewGuid()));
        }

        [Fact]
        public void DeleteFailsWhenIdNotMatchAkaDocumentAlreadyDeleted()
        {
            Document<Entity>();
            
            var id = Guid.NewGuid();
            var table = store.Configuration.GetDesignFor<Entity>();
            var etag = store.Insert(table.Table, id, new { });

            Should.Throw<ConcurrencyException>(() => store.Delete(table.Table, Guid.NewGuid(), etag));
        }

        [Fact]
        public void CanBatchCommandsAndGetEtag()
        {
            Document<Entity>().With(x => x.Field);
            
            var id1 = Guid.NewGuid();
            var id2 = Guid.NewGuid();
            var table = store.Configuration.GetDesignFor<Entity>();
            var etag = store.Execute(new InsertCommand(table.Table, id1, new { Field = "A" }),
                                     new InsertCommand(table.Table, id2, new { Field = "B" }));

            var rows = database.RawQuery<Guid>("select Etag from #Entities order by Field").ToList();
            rows.Count.ShouldBe(2);
            rows[0].ShouldBe(etag);
            rows[1].ShouldBe(etag);
        }

        [Fact]
        public void BatchesAreTransactional()
        {
            Document<Entity>().With(x => x.Field);
            
            var id1 = Guid.NewGuid();
            var table = store.Configuration.GetDesignFor<Entity>();
            var etagThatMakesItFail = Guid.NewGuid();
            try
            {
                store.Execute(new InsertCommand(table.Table, id1, new { Field = "A" }),
                              new UpdateCommand(table.Table, id1, etagThatMakesItFail, new { Field = "B" }, false));
            }
            catch (ConcurrencyException)
            {
                // ignore the exception and ensure that nothing was inserted
            }

            database.RawQuery<dynamic>("select * from #Entities").Count().ShouldBe(0);
        }

        [Fact]
        public void CanSplitLargeCommandBatches()
        {
            Document<Entity>().With(x => x.Field);
            
            var table = store.Configuration.GetDesignFor<Entity>();

            // the initial migrations might issue some requests
            var initialNumberOfRequest = store.NumberOfRequests;

            var commands = new List<DatabaseCommand>();
            for (var i = 0; i < 2100/4 + 1; i++)
            {
                commands.Add(new InsertCommand(table.Table, Guid.NewGuid(), new { Field = "A", Document = documentAsByteArray }));
            }

            store.Execute(commands.ToArray());
            (store.NumberOfRequests - initialNumberOfRequest).ShouldBe(2);
        }

        [Fact]
        public void CanStoreAndQueryEnumProjection()
        {
            Document<Entity>().With(x => x.EnumProp);
            
            var table = store.Configuration.GetDesignFor<Entity>();
            var id = Guid.NewGuid();
            store.Insert(table.Table, id, new { EnumProp = SomeFreakingEnum.Two });

            var result = store.Get(table.Table, id);
            result[table.Table["EnumProp"]].ShouldBe(SomeFreakingEnum.Two.ToString());
        }

        [Fact]
        public void CanStoreAndQueryEnumProjectionToNetType()
        {
            Document<Entity>().With(x => x.EnumProp);
            
            var table = store.Configuration.GetDesignFor<Entity>();
            var id = Guid.NewGuid();
            store.Insert(table.Table, id, new { EnumProp = SomeFreakingEnum.Two });

            QueryStats stats;
            var result = store.Query<ProjectionWithEnum>(table.Table, out stats).Single();
            result.EnumProp.ShouldBe(SomeFreakingEnum.Two);
        }

        [Fact]
        public void CanStoreAndQueryStringProjection()
        {
            Document<Entity>().With(x => x.Property);
            
            var table = store.Configuration.GetDesignFor<Entity>();
            var id = Guid.NewGuid();
            store.Insert(table.Table, id, new { Property = "Hest" });

            var result = store.Get(table.Table, id);
            result[table.Table["Property"]].ShouldBe("Hest");
        }

        [Fact]
        public void CanStoreAndQueryOnNull()
        {
            Document<Entity>().With(x => x.Property);
            
            var table = store.Configuration.GetDesignFor<Entity>();
            var id = Guid.NewGuid();
            store.Insert(table.Table, id, new { Property = (string)null });

            QueryStats stats;
            var result = store.Query(table.Table, out stats, where: "(@Value IS NULL AND Property IS NULL) OR Property = @Value", parameters: new { Value = (string)null });
            result.Count().ShouldBe(1);
        }

        [Fact]
        public void CanStoreAndQueryDateTimeProjection()
        {
            Document<Entity>().With(x => x.DateTimeProp);
            
            var table = store.Configuration.GetDesignFor<Entity>();
            var id = Guid.NewGuid();
            store.Insert(table.Table, id, new { DateTimeProp = new DateTime(2001, 12, 24, 1, 1, 1) });

            QueryStats stats;
            var result = store.Query(table.Table, out stats, where: "DateTimeProp = @dtp", parameters: new { dtp = new DateTime(2001, 12, 24, 1, 1, 1) });
            result.First()[table.Table["DateTimeProp"]].ShouldBe(new DateTime(2001, 12, 24, 1, 1, 1));
        }

        [Fact]
        public void CanPage()
        {
            Document<Entity>().With(x => x.Number);
            
            var table = store.Configuration.GetDesignFor<Entity>();
            for (var i = 0; i < 10; i++)
                store.Insert(table.Table, Guid.NewGuid(), new { Number = i });

            QueryStats stats;
            var result = store.Query(table.Table, out stats, window: new SkipTake(2, 5), orderby: "Number").ToList();

            result.Count.ShouldBe(5);
            var props = result.Select(x => x[table.Table["Number"]]).ToList();
            props.ShouldContain(2);
            props.ShouldContain(3);
            props.ShouldContain(4);
            props.ShouldContain(5);
            props.ShouldContain(6);
            stats.TotalResults.ShouldBe(10);
        }

        [Fact]
        public void CanTake()
        {
            Document<Entity>().With(x => x.Number);
            
            var table = store.Configuration.GetDesignFor<Entity>();
            for (var i = 0; i < 10; i++)
                store.Insert(table.Table, Guid.NewGuid(), new { Number = i });

            QueryStats stats;
            var result = store.Query(table.Table, out stats, window: new SkipTake(0, 2), orderby: "Number").ToList();

            result.Count.ShouldBe(2);
            var props = result.Select(x => x[table.Table["Number"]]).ToList();
            props.ShouldContain(0);
            props.ShouldContain(1);
            stats.TotalResults.ShouldBe(10);
        }

        [Fact]
        public void CanSkip()
        {
            Document<Entity>().With(x => x.Number);
            
            var table = store.Configuration.GetDesignFor<Entity>();
            for (var i = 0; i < 10; i++)
                store.Insert(table.Table, Guid.NewGuid(), new { Number = i });

            QueryStats stats;
            var result = store.Query(table.Table, out stats, window: new SkipTake(7, 0), orderby: "Number").ToList();

            result.Count.ShouldBe(3);
            var props = result.Select(x => x[table.Table["Number"]]).ToList();
            props.ShouldContain(7);
            props.ShouldContain(8);
            props.ShouldContain(9);
            stats.TotalResults.ShouldBe(10);
        }

        [Theory]
        [InlineData(0, new[] { 0, 1, 2, 3, 4 })]
        [InlineData(3, new[] { 0, 1, 2, 3, 4 })]
        [InlineData(4, new[] { 0, 1, 2, 3, 4 })]
        [InlineData(5, new[] { 5, 6, 7, 8, 9 })]
        [InlineData(7, new[] { 5, 6, 7, 8, 9 })]
        [InlineData(9, new[] { 5, 6, 7, 8, 9 })]
        [InlineData(10, new[] { 10, 11, 12, 13, 14 })]
        [InlineData(15, new[] { 15, 16 })]
        [InlineData(16, new[] { 15, 16 })]
        public void CanSkipToId(int index, int[] expected)
        {
            Document<Entity>().With(x => x.Number);

            var ids = new List<Guid>();
            var table = store.Configuration.GetDesignFor<Entity>();
            for (var i = 0; i < 17; i++)
            {
                var id = Guid.NewGuid();
                ids.Add(id);
                store.Insert(table.Table, id, new { Number = i });
            }

            QueryStats stats;
            var result = store.Query(table.Table, out stats, window: new SkipToId(ids[index], 5), orderby: "Number").ToList();

            result.Select(x => (int)x[table.Table["Number"]])
                .ShouldBe(expected);

            stats.TotalResults.ShouldBe(17);
            stats.RetrievedResults.ShouldBe(expected.Length);
            stats.FirstRowNumberOfWindow.ShouldBe(expected[0]);
        }

        [Theory]
        [InlineData(0, 0, new[] { 0, 2, 4, 6, 8 })]
        [InlineData(2, 0, new[] { 0, 2, 4, 6, 8 })]
        [InlineData(3, 0, new[] { 0, 2, 4, 6, 8 })]
        [InlineData(10, 5, new[] { 10, 12, 14, 16 })]
        [InlineData(11, 0, new[] { 0, 2, 4, 6, 8 })]
        public void CanSkipToId_Where(int index, int firstRowOfWindow, int[] expected)
        {
            Document<Entity>().With(x => x.Number);

            var ids = new List<Guid>();
            var table = store.Configuration.GetDesignFor<Entity>();
            for (var i = 0; i < 17; i++)
            {
                var id = Guid.NewGuid();
                ids.Add(id);
                store.Insert(table.Table, id, new { Number = i });
            }

            QueryStats stats;
            var result = store.Query(table.Table, out stats, where: "Number % 2 = 0", window: new SkipToId(ids[index], 5), orderby: "Number").ToList();

            result.Select(x => (int)x[table.Table["Number"]])
                .ShouldBe(expected);

            stats.TotalResults.ShouldBe(9);
            stats.RetrievedResults.ShouldBe(expected.Length);
            stats.FirstRowNumberOfWindow.ShouldBe(firstRowOfWindow);
        }

        [Fact]
        public void CanSkipToId_Where_NoResults()
        {
            Document<Entity>().With(x => x.Number);

            var ids = new List<Guid>();
            var table = store.Configuration.GetDesignFor<Entity>();
            for (var i = 0; i < 17; i++)
            {
                var id = Guid.NewGuid();
                ids.Add(id);
                store.Insert(table.Table, id, new { Number = i });
            }

            QueryStats stats;
            var result = store.Query(table.Table, out stats, where: "Number = -1", window: new SkipToId(ids[0], 5), orderby: "Number").ToList();

            result.Select(x => (int)x[table.Table["Number"]])
                .ShouldBeEmpty();

            stats.TotalResults.ShouldBe(0);
            stats.RetrievedResults.ShouldBe(0);
            stats.FirstRowNumberOfWindow.ShouldBe(0);
        }

        [Fact]
        public void CanQueryWithoutWhere()
        {
            Document<Entity>();
            
            var table = store.Configuration.GetDesignFor<Entity>();
            store.Insert(table.Table, Guid.NewGuid(), new { });

            QueryStats stats;
            var result = store.Query(table.Table, out stats).ToList();

            result.Count.ShouldBe(1);
        }

        [Fact]
        public void CanGetStats()
        {
            Document<Entity>().With(x => x.Property);
            
            var table = store.Configuration.GetDesignFor<Entity>();
            for (var i = 0; i < 10; i++)
                store.Insert(table.Table, Guid.NewGuid(), new { Property = i });

            QueryStats stats;
            store.Query(table.Table, out stats, where: "Property >= 5");

            stats.RetrievedResults.ShouldBe(5);
            stats.TotalResults.ShouldBe(5);
        }

        [Fact]
        public void CanGetStatsWhenSkipping()
        {
            Document<Entity>().With(x => x.Property);
            
            var table = store.Configuration.GetDesignFor<Entity>();
            for (var i = 0; i < 10; i++)
                store.Insert(table.Table, Guid.NewGuid(), new { Property = i });

            QueryStats stats;
            store.Query(table.Table, out stats, where: "Property >= 5", window: new SkipTake(1, 0));

            stats.RetrievedResults.ShouldBe(4);
            stats.TotalResults.ShouldBe(5);
        }

        [Fact]
        public void CanGetStatsWithNoResults()
        {
            Document<Entity>();
            
            var table = store.Configuration.GetDesignFor<Entity>();

            QueryStats stats;
            store.Query(table.Table, out stats);

            stats.RetrievedResults.ShouldBe(0);
            stats.TotalResults.ShouldBe(0);
        }

        [Fact]
        public void CanGetStatsWhenOrderingByPropertyWithSameValue()
        {
            Document<Entity>().With(x => x.Property);
            
            var table = store.Configuration.GetDesignFor<Entity>();
            store.Insert(table.Table, Guid.NewGuid(), new { Property = 10 });
            store.Insert(table.Table, Guid.NewGuid(), new { Property = 10 });
            store.Insert(table.Table, Guid.NewGuid(), new { Property = 10 });
            store.Insert(table.Table, Guid.NewGuid(), new { Property = 10 });
            store.Insert(table.Table, Guid.NewGuid(), new { Property = 11 });
            store.Insert(table.Table, Guid.NewGuid(), new { Property = 11 });

            QueryStats stats;
            store.Query(table.Table, out stats, @orderby: "Property", window: new SkipTake(1, 0));
            
            stats.RetrievedResults.ShouldBe(5);
            stats.TotalResults.ShouldBe(6);
        }

        [Fact]
        public void CanGetStatsWhenSkippingAllOrMore()
        {
            Document<Entity>().With(x => x.Property);
            
            var table = store.Configuration.GetDesignFor<Entity>();
            for (var i = 0; i < 10; i++)
                store.Insert(table.Table, Guid.NewGuid(), new { Property = i });

            QueryStats stats;
            store.Query(table.Table, out stats, where: "Property >= 5", window: new SkipTake(10, 0));

            stats.RetrievedResults.ShouldBe(0);
            stats.TotalResults.ShouldBe(5);
        }

        [Fact]
        public void CanGetStatsWhenTaking()
        {
            Document<Entity>().With(x => x.Property);
            
            var table = store.Configuration.GetDesignFor<Entity>();
            for (var i = 0; i < 10; i++)
                store.Insert(table.Table, Guid.NewGuid(), new { Property = i });

            QueryStats stats;
            store.Query(table.Table, out stats, where: "Property >= 5", window: new SkipTake(0, 2));

            stats.RetrievedResults.ShouldBe(2);
            stats.TotalResults.ShouldBe(5);
        }

        [Fact]
        public void CanGetStatsWhenTakingAllOrMore()
        {
            Document<Entity>().With(x => x.Property);
            
            var table = store.Configuration.GetDesignFor<Entity>();
            for (var i = 0; i < 10; i++)
                store.Insert(table.Table, Guid.NewGuid(), new { Property = i });

            QueryStats stats;
            store.Query(table.Table, out stats, where: "Property >= 5", window: new SkipTake(0, 20));

            stats.RetrievedResults.ShouldBe(5);
            stats.TotalResults.ShouldBe(5);
        }

        [Fact]
        public void CanOrderBy()
        {
            Document<Entity>().With(x => x.Field);
            
            var table = store.Configuration.GetDesignFor<Entity>();
            for (var i = 5; i > 0; i--)
                store.Insert(table.Table, Guid.NewGuid(), new { Field = i });

            QueryStats stats;
            var result = store.Query(table.Table, out stats, orderby: "Field").ToList();

            var props = result.Select(x => x[table.Table["Field"]]).ToList();
            props[0].ShouldBe("1");
            props[1].ShouldBe("2");
            props[2].ShouldBe("3");
            props[3].ShouldBe("4");
            props[4].ShouldBe("5");
        }

        [Fact]
        public void CanOrderByIdAndSelectOtherField()
        {
            Document<Entity>().With(x => x.Field);
            
            var table = store.Configuration.GetDesignFor<Entity>();
            for (var i = 5; i > 0; i--)
                store.Insert(table.Table, new Guid("00000000-0000-0000-0000-00000000000" + i), new { Field = i });

            QueryStats stats;
            var result = store.Query(table.Table, out stats, select: "Field", orderby: "Id").ToList();

            var props = result.Select(x => x[table.Table["Field"]]).ToList();
            props[0].ShouldBe("1");
            props[1].ShouldBe("2");
            props[2].ShouldBe("3");
            props[3].ShouldBe("4");
            props[4].ShouldBe("5");
        }

        [Fact]
        public void CanOrderByIdAndSelectOtherFieldWindowed()
        {
            Document<Entity>().With(x => x.Field);
            
            var table = store.Configuration.GetDesignFor<Entity>();
            for (var i = 5; i > 0; i--)
                store.Insert(table.Table, new Guid("00000000-0000-0000-0000-00000000000" + i), new { Field = i });

            QueryStats stats;
            var result = store.Query(table.Table, out stats, select: "Field", orderby: "Id", window: new SkipTake(1, 1)).Single();

            result[table.Table["Field"]].ShouldBe("2");
        }

        [Fact]
        public void CanOrderByDescWhileSkippingAndTaking()
        {
            Document<Entity>().With(x => x.Field);
            
            var table = store.Configuration.GetDesignFor<Entity>();
            for (var i = 5; i > 0; i--)
                store.Insert(table.Table, Guid.NewGuid(), new { Field = i });

            QueryStats stats;
            var result = store.Query(table.Table, out stats, window: new SkipTake(2, 2), orderby: "Field desc").ToList();

            var props = result.Select(x => x[table.Table["Field"]]).ToList();
            props[0].ShouldBe("3");
            props[1].ShouldBe("2");
        }

        [Fact]
        public void FailsIfEntityTypeIsUnknown()
        {
            Should.Throw<HybridDbException>(() => store.Configuration.GetDesignFor<int>());
        }

        [Fact]
        public void WillEnlistCommandsInAmbientTransactions()
        {
            Document<Entity>();

            var table = store.Configuration.GetDesignFor<Entity>();

            using (new TransactionScope())
            {
                store.Insert(table.Table, Guid.NewGuid(), new { });
                store.Insert(table.Table, Guid.NewGuid(), new { });

                // No tx complete here
            }

            database.RawQuery<dynamic>("select * from #Entities").Count().ShouldBe(0);
        }

        [Fact]
        public void CanUseGlobalTempTables()
        {
            using (var globalStore1 = DocumentStore.ForTesting(
                TableMode.UseGlobalTempTables,
                configurator: new LambdaHybridDbConfigurator(x => x.Document<Case>())))
            {
                var id = Guid.NewGuid();
                globalStore1.Insert(globalStore1.Configuration.GetDesignFor<Case>().Table, id, new { });

                using (var globalStore2 = DocumentStore.ForTesting(TableMode.UseGlobalTempTables))
                {
                    globalStore2.Configuration.Document<Case>();
                    var result = globalStore2.Get(globalStore2.Configuration.GetDesignFor<Case>().Table, id);

                    result.ShouldNotBe(null);
                }
            }

            var tables = database.RawQuery<string>(string.Format("select OBJECT_ID('##Cases') as Result"));
            tables.First().ShouldBe(null);
        }

        [Fact]
        public void UtilityColsAreRemovedFromQueryResults()
        {
            Document<Entity>();

            var table = new DocumentTable("Entities");
            store.Insert(table, Guid.NewGuid(), new { Version = 1 });

            QueryStats stats;
            var result1 = store.Query(table, out stats, window: new SkipTake(0, 2)).Single();
            result1.ContainsKey(new Column("RowNumber", typeof(int))).ShouldBe(false);
            result1.ContainsKey(new Column("TotalResults", typeof(int))).ShouldBe(false);

            var result2 = store.Query<object>(table, out stats, window: new SkipTake(0, 2)).Single();
            ((IDictionary<string, object>)result2).ContainsKey("RowNumber").ShouldBe(false);
            ((IDictionary<string, object>)result2).ContainsKey("TotalResults").ShouldBe(false);
        }

        [Fact]
        public void CanQueryWithConcatenation()
        {
            Document<Entity>().With(x => x.Property);
            Document<OtherEntityWithSomeSimilarities>().With(x => x.Property);
        }


        public class Case
        {
            public Guid Id { get; private set; }
            public string By { get; set; }
        }


        public class OtherEntityWithSomeSimilarities
        {
            public Guid Id { get; set; }
            public int Property { get; set; }
            public string StringProp { get; set; }
        }

        public class ProjectionWithNestedProperty
        {
            public double TheChildNestedDouble { get; set; }
        }

        public class ProjectionWithEnum
        {
            public SomeFreakingEnum EnumProp { get; set; }
        }

        public class ProjectionWithNonProjectedField
        {
            public string NonProjectedField { get; set; }
        }

        public class EntityIndex
        {
            public string StringProp { get; set; }
        }

        public class ThrowingHybridDbExtension : IHybridDbExtension
        {
            public void OnRead(Table table, IDictionary<string, object> projections)
            {
                throw new OperationException();
            }

            public class OperationException : Exception { }
        }

        public class CountingSerializer : ISerializer
        {
            readonly ISerializer serializer;

            public CountingSerializer(ISerializer serializer)
            {
                this.serializer = serializer;
            }

            public int DeserializeCount { get; private set; }

            public byte[] Serialize(object obj)
            {
                return serializer.Serialize(obj);
            }

            public object Deserialize(byte[] data, Type type)
            {
                DeserializeCount++;
                return serializer.Deserialize(data, type);
            }
        }
    }

}