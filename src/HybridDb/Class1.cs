using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Reflection.Metadata;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using ShinySwitch;
using static HybridDb.Helpers;

namespace HybridDb
{
    // Planer og tanker:
    // Etags skal være scalars, der tæller op. Vi kan senere måle på performance for dette. Husk index på denne.
    // Lav oprettelse af Documents-tabellen som den første migration
    // Fælles config skal have en klasse og sendes ind i ctor på DocumentStore
    // LinqProvideren skal kunne bruges uden DocumentSession
    // Lav et plugin-system til linq-providers
    // Migreringer skal flere typer commands, så forskellige stores kan reagere forskelligt på dem
    // Up skal kunne køres flere gange
    // gem alle migrationnavne (måske deres sql hvis den er der) som rows i HybridDb tabellen


    public interface IDocumentStore
    {
        void Up(params Migration[] migrations);
        Task<long> Execute(IEnumerable<DatabaseCommand> commands);

        Task<QueryResult> Get(string key);

        //IEnumerable<QueryResult<TProjection>> Query<TProjection>(
        //    Table table, out QueryStats stats, string select = "",
        //    string where = "", int skip = 0, int take = 0,
        //    string orderby = "", bool includeDeleted = false, object parameters = null);
    }

    public class DocumentStore : IDocumentStore
    {
        public string ConnectionString { get; }

        public DocumentStore(string connectionString) => ConnectionString = connectionString;

        public void Up(params Migration[] migrations)
        {
            var all = ListOf(new Migration(0,
                new SqlMigrationCommand("", new { })));

            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();

                using (var tx = connection.BeginTransaction(IsolationLevel.Serializable))
                {
                    connection.Execute(@"
                        if object_id('HybridDb', 'U') is null
                        begin
                            create table HybridDb (SchemaVersion int not null default 0)
                            insert into HybridDb (SchemaVersion) values (0)
                        end;

                        if object_id('Documents', 'U') is null
                        begin
                            create table Documents (
                                Id nvarchar(900) not null
                            )
                        end;
                    ", transaction: tx);

                    var schemaVersion = connection.QueryFirst<int>("select SchemaVersion from HybridDb", transaction: tx);

                    foreach (var migration in all.SkipWhile(x => x.Version <= schemaVersion))
                    {
                        foreach (var command in migration.Commands.OfType<SqlMigrationCommand>())
                        {
                            connection.Execute(command.Sql, command.Param, tx);
                        }
                    }

                    tx.Commit();
                }
            }
        }

        public async Task<long> Execute(IEnumerable<DatabaseCommand> commands)
        {
            commands = commands.ToList();

            if (!commands.Any()) return 0;

            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();

                using (var tx = connection.BeginTransaction(IsolationLevel.RepeatableRead))
                {
                    var etag = Guid.NewGuid();

                    foreach (var command in commands)
                    {
                        var preparedCommand = Switch<(string sql, object param)>.On(command)
                            .Match<Insert>(insert => ($@"
                                insert into [Documents] (Id, Etag, Document) 
                                values (@Key, @Document);",
                                new
                                {
                                    insert.Key,
                                    insert.Document
                                }))
                            .OrThrow();

                        var rowcount = await connection.ExecuteAsync(preparedCommand.Item1, preparedCommand.Item2);

                        if (rowcount != 1)
                        {
                            throw new ConcurrencyException(
                                $"Someone beat you to it. Expected one change, but got {rowcount}. " +
                                $"The transaction is rolled back now, so no changes was actually made.");
                        }
                    }

                    tx.Commit();

                    return 0;
                }
            }
        }

        public Task<QueryResult> Get(string key) => throw new NotImplementedException();
    }

    [Serializable]
    public class ConcurrencyException : Exception
    {
        public ConcurrencyException() { }
        public ConcurrencyException(string message) : base(message) { }
        public ConcurrencyException(string message, Exception inner) : base(message, inner) { }

        protected ConcurrencyException(
            SerializationInfo info,
            StreamingContext context) : base(info, context) { }
    }

    public abstract class DatabaseCommand { }

    public class Insert : DatabaseCommand
    {
        public Insert(string key, Dictionary<string, string[]> metadata, string document)
        {
            Key = key;
            Metadata = metadata;
            Document = document;
        }

        public string Key { get; }
        public Dictionary<string, string[]> Metadata { get; }
        public string Document { get; }
    }

    public class Migration
    {
        public Migration(int version, params MigrationCommand[] commands)
        {
            Version = version;
            Commands = commands;
        }

        public int Version { get; }
        public IReadOnlyList<MigrationCommand> Commands { get; }

        //public virtual IEnumerable<SchemaMigrationCommand> MigrateSchema()
        //{
        //    yield break;
        //}

        //public virtual IEnumerable<DocumentMigrationCommand> MigrateDocument()
        //{
        //    yield break;
        //}
    }

    public abstract class MigrationCommand { }

    public class SqlMigrationCommand : MigrationCommand
    {
        public SqlMigrationCommand(string sql, object param)
        {
            Sql = sql;
            Param = param;
        }

        public string Sql { get; }
        public object Param { get; }
    }

    public class QueryStats
    {
        public int RetrievedResults { get; set; }
        public int TotalResults { get; set; }
        public long QueryDurationInMilliseconds { get; set; }

        public void CopyTo(QueryStats target)
        {
            target.TotalResults = TotalResults;
        }
    }

    public class QueryResult
    {
        public QueryResult(string key, Dictionary<string, string[]> metadata, string document)
        {
            Key = key;
            Metadata = metadata;
            Document = document;
        }

        public string Key { get; }
        public Dictionary<string, string[]> Metadata { get; }
        public string Document { get; }
    }

    [Flags]
    public enum Operation
    {
        Inserted = 1,
        Updated = 2,
        Deleted = 4
    }

    //public class DocumentTable : Table
    //{
    //    public DocumentTable(string name) : base(name)
    //    {
    //        IdColumn = new Column("Id", typeof(string), length: 900, isPrimaryKey: true);
    //        Register(IdColumn);

    //        EtagColumn = new Column("Etag", typeof(Guid));
    //        Register(EtagColumn);

    //        CreatedAtColumn = new Column("CreatedAt", typeof(DateTimeOffset));
    //        Register(CreatedAtColumn);

    //        ModifiedAtColumn = new Column("ModifiedAt", typeof(DateTimeOffset));
    //        Register(ModifiedAtColumn);

    //        DocumentColumn = new Column("Document", typeof(byte[]));
    //        Register(DocumentColumn);

    //        MetadataColumn = new Column("Metadata", typeof(byte[]));
    //        Register(MetadataColumn);

    //        DiscriminatorColumn = new Column("Discriminator", typeof(string), length: 900);
    //        Register(DiscriminatorColumn);

    //        AwaitsReprojectionColumn = new Column("AwaitsReprojection", typeof(bool));
    //        Register(AwaitsReprojectionColumn);

    //        VersionColumn = new Column("Version", typeof(int));
    //        Register(VersionColumn);

    //        RowVersionColumn = new Column("VirtualTime", SqlDbType.Timestamp, typeof(int));
    //        Register(RowVersionColumn);

    //        LastOperationColumn = new Column("LastOperation", SqlDbType.TinyInt, typeof(byte));
    //        Register(LastOperationColumn);
    //    }

    //    public Column IdColumn { get; }
    //    public Column EtagColumn { get; }
    //    public Column CreatedAtColumn { get; }
    //    public Column ModifiedAtColumn { get; }
    //    public Column DocumentColumn { get; }
    //    public Column MetadataColumn { get; }
    //    public Column DiscriminatorColumn { get; }
    //    public Column AwaitsReprojectionColumn { get; }
    //    public Column VersionColumn { get; }
    //    public Column RowVersionColumn { get; }
    //    public Column LastOperationColumn { get; }
    //}

    //public class Table
    //{
    //    readonly Dictionary<string, Column> columns;

    //    public Table(string name) : this(name, Enumerable.Empty<Column>()) { }
    //    public Table(string name, params Column[] columns) : this(name, columns.ToList()) { }

    //    public Table(string name, IEnumerable<Column> columns)
    //    {
    //        if (name.EndsWith("_"))
    //        {
    //            throw new NotSupportedException("A table name can not end with '_'.");
    //        }

    //        Name = name;

    //        this.columns = columns.ToDictionary(x => x.Name, x => x);
    //    }

    //    public Column this[string name] => columns.TryGetValue(name, out var value) ? value : null;

    //    public virtual Column this[KeyValuePair<string, object> namedValue] => this[namedValue.Key];

    //    public string Name { get; }

    //    public IEnumerable<Column> Columns => columns.Values;

    //    public void Register(Column column) => columns.Add(column.Name, column);

    //    public override string ToString() => Name;
    //}
}