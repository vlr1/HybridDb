﻿using System;
using System.Linq;
using HybridDb.Schema;

namespace HybridDb.Commands
{
    public class UpdateCommand : DatabaseCommand
    {
        readonly Guid currentEtag;
        readonly byte[] document;
        readonly Guid key;
        readonly object projections;
        readonly bool lastWriteWins;
        readonly ITable table;

        public UpdateCommand(ITable table, Guid key, Guid etag, byte[] document, object projections, bool lastWriteWins)
        {
            this.table = table;
            this.key = key;
            currentEtag = etag;
            this.document = document;
            this.projections = projections;
            this.lastWriteWins = lastWriteWins;
        }

        public byte[] Document
        {
            get { return document; }
        }

        internal override PreparedDatabaseCommand Prepare(DocumentStore store, Guid etag, int uniqueParameterIdentifier)
        {
            var values = ConvertAnonymousToProjections(table, projections);

            values.Add(table.EtagColumn, etag);
            values.Add(table.DocumentColumn, document);

            var sql = new SqlBuilder()
                .Append("update {0} set {1} where {2}=@Id{3}",
                        store.Escape(store.GetFormattedTableName(table)),
                        string.Join(", ", from column in values.Keys select column.Name + "=@" + column.Name + uniqueParameterIdentifier),
                        table.IdColumn.Name,
                        uniqueParameterIdentifier)
                .Append(!lastWriteWins, "and {0}=@CurrentEtag{1}",
                        table.EtagColumn.Name,
                        uniqueParameterIdentifier)
                .ToString();

            var parameters = MapProjectionsToParameters(values, uniqueParameterIdentifier);
            parameters.Add(new Parameter { Name = "@Id" + uniqueParameterIdentifier, Value = key, DbType = table.IdColumn.SqlColumn.Type });

            if (!lastWriteWins)
            {
                parameters.Add(new Parameter { Name = "@CurrentEtag" + uniqueParameterIdentifier, Value = currentEtag, DbType = table.EtagColumn.SqlColumn.Type });
            }

            return new PreparedDatabaseCommand
            {
                Sql = sql,
                Parameters = parameters,
                ExpectedRowCount = 1
            };
        }
    }
}