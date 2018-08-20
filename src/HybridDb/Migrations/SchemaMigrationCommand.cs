using System;
using System.Data;
using System.Data.SqlClient;
using HybridDb.Config;

namespace HybridDb.Migrations
{
    public abstract class SchemaMigrationCommand
    {
        protected SchemaMigrationCommand()
        {
            Unsafe = false;
            RequiresReprojectionOf = null;
        }

        public bool Unsafe { get; protected set; }
        public string RequiresReprojectionOf { get; protected set; }

        public abstract void Execute(IDatabase database);
        public new abstract string ToString();

        protected string GetTableExistsSql(IDatabase db, string tablename) => $"object_id('{db.FormatTableName(tablename)}', 'U') is not null";

        protected SqlBuilder GetColumnSqlType(Column column, string defaultValuePostfix = "", bool inMem = false)
        {
            if (column.Type == null)
                throw new ArgumentException($"Column {column.Name} must have a type");

            var sql = new SqlBuilder();

            var sqlColumn = SqlTypeMap.Convert(column);
            sql.Append(column.DbType.ToString());
            sql.Append(sqlColumn.Length != null, "(" + sqlColumn.Length + ")");
            sql.Append(column.Nullable, "NULL").Or("NOT NULL");
            sql.Append(column.DefaultValue != null, $"DEFAULT '{column.DefaultValue}'");
            sql.Append(column.IsPrimaryKey, $" PRIMARY KEY {(inMem ? "NONCLUSTERED HASH WITH (BUCKET_COUNT = 100000)" : "")}");

            return sql;
        }
    }
}