using System;
using System.Collections.Generic;
using System.Linq;
using HybridDb.Schema;

namespace HybridDb.Commands
{
    public class InsertCommand : DatabaseCommand
    {
        readonly DocumentTable documentTable;
        readonly Guid key;
        readonly byte[] document;
        readonly object projections;

        public InsertCommand(DocumentTable documentTable, Guid key, byte[] document, object projections)
        {
            this.documentTable = documentTable;
            this.key = key;
            this.document = document;
            this.projections = projections;
        }

        internal override PreparedDatabaseCommand Prepare(DocumentStore store, Guid etag, int uniqueParameterIdentifier)
        {
            var documentColumns = new Dictionary<Column, object>();
            documentColumns[documentTable.IdColumn] = key;
            documentColumns[documentTable.EtagColumn] = etag;
            documentColumns[documentTable.CreatedAtColumn] = DateTimeOffset.Now;
            documentColumns[documentTable.ModifiedAtColumn] = DateTimeOffset.Now;
            documentColumns[documentTable.DocumentColumn] = document;

            var sql = string.Format("insert into {0} ({1}) values ({2}); set @rowcount = @rowcount + @@ROWCOUNT;",
                store.FormatTableNameAndEscape(documentTable.Name),
                string.Join(", ", from column in documentColumns.Keys select column.Name),
                string.Join(", ", from column in documentColumns.Keys select "@" + column.Name + uniqueParameterIdentifier));

            var parameters = MapProjectionsToParameters(documentColumns, uniqueParameterIdentifier.ToString());

            var indexTable = store.Configuration.IndexTable;
            var projectionsAsDictionary = projections as IDictionary<string, object> ?? ObjectToDictionaryRegistry.Convert(projections);
            foreach (var projection in projectionsAsDictionary)
            {
                var indexColumns = new Dictionary<Column, object>();
                indexColumns[indexTable.DocumentIdColumn] = key;
                indexColumns[indexTable.DocumentTypeColumn] = documentTable.Name;
                indexColumns[indexTable.PropertyColumn] = projection.Key;
                indexColumns[indexTable.StringValueColumn] = projection.Value;

                sql += string.Format("insert into {0} ({1}) values ({2});",
                    store.FormatTableNameAndEscape(indexTable.Name),
                    string.Join(", ", from column in indexColumns.Keys select column.Name),
                    string.Join(", ", from column in indexColumns.Keys select "@" + column.Name + projection.Key + uniqueParameterIdentifier));

                parameters = parameters.Concat(MapProjectionsToParameters(indexColumns, projection.Key + uniqueParameterIdentifier)).ToDictionary();
            }

            return new PreparedDatabaseCommand
            {
                Sql = sql,
                Parameters = parameters.Values.ToList(),
                ExpectedRowCount = 1
            };
        }
    }
}