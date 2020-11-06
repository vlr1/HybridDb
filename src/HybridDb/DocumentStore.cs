using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using Dapper;
using HybridDb.Commands;
using HybridDb.Config;
using HybridDb.Migrations;
using Serilog;

namespace HybridDb
{
    public class DocumentStore : IDocumentStore
    {
        Guid lastWrittenEtag;
        long numberOfRequests;

        DocumentStore(Database database, Configuration configuration)
        {
            Logger = configuration.Logger;
            Database = database;
            Configuration = configuration;
        }

        internal DocumentStore(DocumentStore store, Configuration configuration) : this(store.Database, configuration) { }

        public static IDocumentStore Create(string connectionString, IHybridDbConfigurator configurator = null)
        {
            configurator = configurator ?? new NullHybridDbConfigurator();
            var configuration = configurator.Configure();
            var database = new Database(configuration.Logger, connectionString, TableMode.UseRealTables);
            var store = new DocumentStore(database, configuration);
            new SchemaMigrationRunner(store, new SchemaDiffer()).Run();
            new DocumentMigrationRunner(store).RunInBackground();
            return store;
        }

        public static IDocumentStore ForTesting(TableMode mode, IHybridDbConfigurator configurator = null)
        {
            return ForTesting(mode, null, configurator);
        }

        public static IDocumentStore ForTesting(TableMode mode, string connectionString, IHybridDbConfigurator configurator = null)
        {
            configurator = configurator ?? new NullHybridDbConfigurator();
            var configuration = configurator.Configure();
            var database = new Database(configuration.Logger, connectionString ?? "data source=.;Integrated Security=True", mode);
            return ForTesting(database, configuration);
        }

        public static IDocumentStore ForTesting(Database database, Configuration configuration)
        {
            var store = new DocumentStore(database, configuration);
            new SchemaMigrationRunner(store, new SchemaDiffer()).Run();
            new DocumentMigrationRunner(store).RunSynchronously();
            return store;
        }

        public Database Database { get; private set; }
        public ILogger Logger { get; private set; }
        public Configuration Configuration { get; private set; }

        public IDocumentSession OpenSession()
        {
            return new DocumentSession(this);
        }

        public Guid Execute(IEnumerable<DatabaseCommand> commands)
        {
            commands = commands.ToList();

            if (!commands.Any())
                return LastWrittenEtag;

            var timer = Stopwatch.StartNew();
            using (var connectionManager = Database.Connect())
            {
                var i = 0;
                var etag = Guid.NewGuid();
                var sql = "";
                var parameters = new List<Parameter>();
                var numberOfParameters = 0;
                var expectedRowCount = 0;
                var numberOfInsertCommands = 0;
                var numberOfUpdateCommands = 0;
                var numberOfDeleteCommands = 0;
                foreach (var command in commands)
                {
                    if (command is InsertCommand)
                        numberOfInsertCommands++;

                    if (command is UpdateCommand)
                        numberOfUpdateCommands++;

                    if (command is DeleteCommand)
                        numberOfDeleteCommands++;

                    var preparedCommand = command.Prepare(this, etag, i++);
                    var numberOfNewParameters = preparedCommand.Parameters.Count;

                    if (numberOfNewParameters >= 2100)
                        throw new InvalidOperationException("Cannot execute a query with more than 2100 parameters.");

                    if (numberOfParameters + numberOfNewParameters >= 2100)
                    {
                        InternalExecute(connectionManager, sql, parameters, expectedRowCount);

                        sql = "";
                        parameters = new List<Parameter>();
                        expectedRowCount = 0;
                        numberOfParameters = 0;
                    }

                    expectedRowCount += preparedCommand.ExpectedRowCount;
                    numberOfParameters += numberOfNewParameters;

                    sql += string.Format("{0};", preparedCommand.Sql);
                    parameters.AddRange(preparedCommand.Parameters);
                }

                InternalExecute(connectionManager, sql, parameters, expectedRowCount);

                connectionManager.Complete();

                Logger.Debug("Executed {0} inserts, {1} updates and {2} deletes in {3}ms",
                    numberOfInsertCommands,
                    numberOfUpdateCommands,
                    numberOfDeleteCommands,
                    timer.ElapsedMilliseconds);

                lastWrittenEtag = etag;
                return etag;
            }
        }

        void InternalExecute(ManagedConnection managedConnection, string sql, List<Parameter> parameters, int expectedRowCount)
        {
            var fastParameters = new FastDynamicParameters(parameters);
            var rowcount = managedConnection.Connection.Execute(sql, fastParameters);
            Interlocked.Increment(ref numberOfRequests);
            if (rowcount != expectedRowCount)
                throw new ConcurrencyException();
        }

        public IEnumerable<TProjection> Query<TProjection>(
            DocumentTable table, out QueryStats stats, bool top1 = false, string select = null, string where = "",
            Window window = null, string orderby = "", object parameters = null)
        {
            if (select.IsNullOrEmpty() || select == "*")
                select = "";

            if (!typeof(TProjection).IsA<IDictionary<string, object>>())
            {
                select = MatchSelectedColumnsWithProjectedType<TProjection>(select);
            }

            IEnumerable<TProjection> result = new List<TProjection>();

            var timer = Stopwatch.StartNew();
            using (var connection = Database.Connect())
            {
                var sql = new SqlBuilder();

                var isWindowed = window != null;

                if (isWindowed)
                {
                    sql.Append("select count(*) as TotalResults")
                        .Append("from {0}", Database.FormatTableNameAndEscape(table.Name))
                        .Append(!string.IsNullOrEmpty(@where), "where {0}", @where);

                    sql.Append(@"; with WithRowNumber as (select *")
                        .Append($", row_number() over(ORDER BY {(string.IsNullOrEmpty(@orderby) ? "CURRENT_TIMESTAMP" : @orderby)})-1 as RowNumber")
                        .Append("from {0}", Database.FormatTableNameAndEscape(table.Name))
                        .Append(!string.IsNullOrEmpty(@where), "where {0}", @where)
                        .Append(")");

                    var skipTake = window as SkipTake;
                    if (skipTake != null)
                    {
                        var skip = skipTake.Skip;
                        var take = skipTake.Take;

                        sql.Append("select {0} {1} from WithRowNumber", top1 ? "top 1" : "", select.IsNullOrEmpty() ? "*" : select + ", RowNumber")
                            .Append("where RowNumber >= {0}", skip)
                            .Append(take > 0, "and RowNumber < {0}", skip + take)
                            .Append("order by RowNumber");
                    }

                    var skipToId = window as SkipToId;
                    if (skipToId != null)
                    {
                        sql.Append("select {0} {1}", top1 ? "top 1" : "", select.IsNullOrEmpty() ? "*" : select + ", RowNumber")
                            .Append("from WithRowNumber")
                            .Append($"where RowNumber >= (select top 1 * from (select RowNumber - (RowNumber % {skipToId.PageSize}) as FirstRow from WithRowNumber where Id=@__Id union all select 0 as FirstRow) as x order by FirstRow desc)")
                            .Append($"and RowNumber < (select top 1 * from (select RowNumber - (RowNumber % {skipToId.PageSize}) as FirstRow from WithRowNumber where Id=@__Id union all select 0 as FirstRow) as x order by FirstRow desc) + {skipToId.PageSize}")
                            .Append("order by RowNumber");

                        sql.Parameters.Add(new Parameter { Name = "@__Id", DbType = DbType.Guid, Value = skipToId.Id });
                    }

                    var internalResult = InternalQuery(connection, sql, parameters, reader => new
                    {
                        Stats = reader.Read<QueryStats>(buffered: true).Single(),
                        Rows = ReadRow<TProjection>(reader)
                    });

                    result = internalResult.Rows.Select(x => x.Data);

                    stats = new QueryStats
                    {
                        TotalResults = internalResult.Stats.TotalResults,
                        RetrievedResults = internalResult.Rows.Count(),
                        FirstRowNumberOfWindow = internalResult.Rows.FirstOrDefault()?.RowNumber ?? 0
                    };
                }
                else
                {
                    if (top1)
                    {
                        sql.Append("select count(*) as TotalResults")
                            .Append("from {0}", Database.FormatTableNameAndEscape(table.Name))
                            .Append(!string.IsNullOrEmpty(@where), "where {0}", @where);

                        sql.Append($"select top 1 {(select.IsNullOrEmpty() ? " * " : select)}, 0 as RowNumber")
                            .Append("from {0}", Database.FormatTableNameAndEscape(table.Name))
                            .Append(!string.IsNullOrEmpty(@where), "where {0}", @where)
                            .Append(!string.IsNullOrEmpty(orderby), "order by {0}", orderby);

                        var internalResult = InternalQuery(connection, sql, parameters, reader => new
                        {
                            Stats = reader.Read<QueryStats>(buffered: true).Single(),
                            Rows = ReadRow<TProjection>(reader)
                        });

                        result = internalResult.Rows.Select(x => x.Data);

                        stats = new QueryStats
                        {
                            TotalResults = internalResult.Stats.TotalResults,
                            RetrievedResults = internalResult.Rows.Count(),
                            FirstRowNumberOfWindow = 0
                        };
                    }
                    else
                    {
                        sql.Append($"select {(select.IsNullOrEmpty() ? " * " : select)}, 0 as RowNumber")
                            .Append("from {0}", Database.FormatTableNameAndEscape(table.Name))
                            .Append(!string.IsNullOrEmpty(@where), "where {0}", @where)
                            .Append(!string.IsNullOrEmpty(orderby), "order by {0}", orderby);

                        result = InternalQuery(connection, sql, parameters, ReadRow<TProjection>).Select(x => x.Data);

                        stats = new QueryStats();
                        stats.TotalResults = stats.RetrievedResults = result.Count();
                        stats.FirstRowNumberOfWindow = 0;
                    }
                }

                stats.QueryDurationInMilliseconds = timer.ElapsedMilliseconds;

                Interlocked.Increment(ref numberOfRequests);

                Logger.Debug("Retrieved {0} of {1} in {2}ms", stats.RetrievedResults, stats.TotalResults, stats.QueryDurationInMilliseconds);

                connection.Complete();

                return result;
            }
        }

        static string MatchSelectedColumnsWithProjectedType<TProjection>(string select)
        {
            var neededColumns = typeof(TProjection).GetProperties().Select(x => x.Name).ToList();
            var selectedColumns =
                from clause in @select.Split(new[] {','}, StringSplitOptions.RemoveEmptyEntries)
                let split = Regex.Split(clause, " AS ", RegexOptions.IgnoreCase).Where(x => x != "").ToArray()
                let column = split[0]
                let alias = split.Length > 1 ? split[1] : null
                where neededColumns.Contains(alias)
                select new {column, alias = alias ?? column};

            var missingColumns =
                from column in neededColumns
                where !selectedColumns.Select(x => x.alias).Contains(column)
                select new {column, alias = column};

            select = string.Join(", ", selectedColumns.Union(missingColumns).Select(x => x.column + " AS " + x.alias));
            return select;
        }

        T InternalQuery<T>(ManagedConnection connection, SqlBuilder sql, object parameters, Func<SqlMapper.GridReader, T> read)
        {
            var p0 = parameters as IEnumerable<Parameter> ?? ConvertToParameters<T>(parameters);
            var p1 = sql.Parameters;

            var normalizedParameters = new FastDynamicParameters(p0.Concat(p1));

            using (var reader = connection.Connection.QueryMultiple(sql.ToString(), normalizedParameters))
            {
                return read(reader);
            }
        }

        public IEnumerable<Row<T>> ReadRow<T>(SqlMapper.GridReader reader)
        {
            if (typeof(T).IsA<IDictionary<string, object>>())
            {
                return (IEnumerable<Row<T>>)reader
                    .Read<object, RowExtras, Row<IDictionary<string, object>>>((a, b) => 
                        CreateRow((IDictionary<string, object>)a, b), "RowNumber", buffered: true);
            }

            return reader.Read<T, RowExtras, Row<T>>(CreateRow, "RowNumber", buffered: true);
        }

        public static Row<T> CreateRow<T>(T data, RowExtras extras) => new Row<T>(data, extras);

        public class RowExtras
        {
            public int RowNumber { get; set; }
        }

        public class Row<T>
        {
            public Row(T data, RowExtras extras)
            {
                Data = data;
                RowNumber = extras.RowNumber;
            }

            public T Data { get; set; }
            public int RowNumber { get; set; }
        }

        static IEnumerable<Parameter> ConvertToParameters<T>(object parameters) =>
            from projection in parameters as IDictionary<string, object> ?? ObjectToDictionaryRegistry.Convert(parameters)
            select new Parameter {Name = "@" + projection.Key, Value = projection.Value};

        public IDictionary<string, object> Get(DocumentTable table, Guid key)
        {
            var timer = Stopwatch.StartNew();
            using (var connection = Database.Connect())
            {
                var sql = string.Format("select * from {0} where {1} = @Id",
                    Database.FormatTableNameAndEscape(table.Name),
                    table.IdColumn.Name);

                var row = ((IDictionary<string, object>) connection.Connection.Query(sql, new {Id = key}).SingleOrDefault());

                Interlocked.Increment(ref numberOfRequests);

                Logger.Debug("Retrieved {0} in {1}ms", key, timer.ElapsedMilliseconds);

                connection.Complete();

                return row;
            }
        }

        public long NumberOfRequests
        {
            get { return numberOfRequests; }
        }

        public Guid LastWrittenEtag
        {
            get { return lastWrittenEtag; }
        }

        public void Dispose()
        {
            Database.Dispose();
        }
    }
}