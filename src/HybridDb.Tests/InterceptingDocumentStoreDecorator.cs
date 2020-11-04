using System;
using System.Collections.Generic;
using HybridDb.Commands;
using HybridDb.Config;

namespace HybridDb.Tests
{
    public class InterceptingDocumentStoreDecorator : IDocumentStore
    {
        readonly IDocumentStore store;

        public InterceptingDocumentStoreDecorator(IDocumentStore store)
        {
            this.store = store;

            OverrideExecute = (s, args) => s.Execute(args);
        }

        public Func<IDocumentStore, IEnumerable<DatabaseCommand>, Guid> OverrideExecute { get; set; } 

        public Configuration Configuration
        {
            get { return store.Configuration; }
        }

        public long NumberOfRequests
        {
            get { return store.NumberOfRequests; }
        }

        public Guid LastWrittenEtag
        {
            get { return store.LastWrittenEtag; }
        }

        public IDocumentSession OpenSession()
        {
            return new DocumentSession(this);
        }

        public Guid Execute(IEnumerable<DatabaseCommand> commands)
        {
            return OverrideExecute(store, commands);
        }

        public IDictionary<string, object> Get(DocumentTable table, Guid key)
        {
            return store.Get(table, key);
        }

        public IEnumerable<TProjection> Query<TProjection>(
            DocumentTable table, out QueryStats stats, bool top1 = false, string @select = "", string @where = "", Window window = null, string @orderby = "", object parameters = null)
        {
            return store.Query<TProjection>(table, out stats, top1, select, where, window, orderby, parameters);
        }

        public void Dispose()
        {
            store.Dispose();
        }
    }
}