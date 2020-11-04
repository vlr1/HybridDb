using System;
using System.Collections.Generic;
using HybridDb.Commands;
using HybridDb.Config;

namespace HybridDb
{
    public interface IDocumentStore : IDisposable
    {
        Configuration Configuration { get; }
        long NumberOfRequests { get; }
        Guid LastWrittenEtag { get; }
        
        IDocumentSession OpenSession();
        Guid Execute(IEnumerable<DatabaseCommand> commands);
        IDictionary<string, object> Get(DocumentTable table, Guid key);
        IEnumerable<TProjection> Query<TProjection>(DocumentTable table, out QueryStats stats, bool top1 = false, string select = "", string where = "", Window window = null, string orderby = "", object parameters = null);
    }
}