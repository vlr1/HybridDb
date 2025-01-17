using System;

namespace HybridDb.Migrations.Schema.Commands
{
    public class SqlCommand : DdlCommand
    {
        public SqlCommand(string description, Action<SqlBuilder, IDatabase> builder, int? commandTimeout = null) : this(description, null, builder, commandTimeout) { }

        public SqlCommand(string description, string requiresReprojectionOf, Action<SqlBuilder, IDatabase> builder, int? commandTimeout = null)
        {
            Safe = true;

            Builder = builder;
            CommandTimeout = commandTimeout;

            Description = description;
            RequiresReprojectionOf = requiresReprojectionOf;
        }

        public Action<SqlBuilder, IDatabase> Builder { get; }
        public int? CommandTimeout { get; }
        public string Description { get; }

        public override string ToString() => Description;

        public override void Execute(DocumentStore store)
        {
            var sql = new SqlBuilder();
            Builder(sql, store.Database);
            store.Database.RawExecute(sql.ToString(), new Parameters(sql.Parameters), commandTimeout: CommandTimeout);
        }
    }
}