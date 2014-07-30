using System;
using System.Data;

namespace HybridDb.Schema
{
    public class IndexTable : Table
    {
        public IndexTable(string name) : base(name)
        {
            DocumentIdColumn = new SystemColumn("DocumentId", new SqlColumn(DbType.Guid));
            Register(DocumentIdColumn);

            DocumentTypeColumn = new SystemColumn("DocumentType", new SqlColumn(DbType.String, 255));
            Register(DocumentTypeColumn);

            PropertyColumn = new SystemColumn("Property", new SqlColumn(DbType.String, 255));
            Register(PropertyColumn);

            StringValueColumn = new SystemColumn("StringValue", new SqlColumn(DbType.String, 255));
            Register(StringValueColumn);
        }

        public SystemColumn DocumentIdColumn { get; private set; }
        public SystemColumn DocumentTypeColumn { get; private set; }
        public SystemColumn PropertyColumn { get; private set; }
        public SystemColumn StringValueColumn { get; private set; }
    }
}