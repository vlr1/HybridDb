using System;
using System.Data;

namespace HybridDb.Schema
{
    public class DocumentTable : Table
    {
        public DocumentTable(string name) : base(name)
        {
            IdColumn = new SystemColumn("Id", new SqlColumn(DbType.Guid, isPrimaryKey: true));
            Register(IdColumn);

            EtagColumn = new SystemColumn("Etag", new SqlColumn(DbType.Guid));
            Register(EtagColumn);

            CreatedAtColumn = new SystemColumn("CreatedAt", new SqlColumn(DbType.DateTimeOffset));
            Register(CreatedAtColumn);

            ModifiedAtColumn = new SystemColumn("ModifiedAt", new SqlColumn(DbType.DateTimeOffset));
            Register(ModifiedAtColumn);

            VersionColumn = new Column("Version", new SqlColumn(DbType.Int32, nullable: true));
            Register(VersionColumn);

            DocumentColumn = new Column("Document", new SqlColumn(DbType.Binary, Int32.MaxValue, nullable: true));
            Register(DocumentColumn);
        }

        public SystemColumn IdColumn { get; private set; }
        public SystemColumn EtagColumn { get; private set; }
        public SystemColumn CreatedAtColumn { get; private set; }
        public SystemColumn ModifiedAtColumn { get; private set; }
        public Column VersionColumn { get; private set; }
        public Column DocumentColumn { get; private set; }
    }
}