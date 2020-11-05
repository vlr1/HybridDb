﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Transactions;
using HybridDb.Config;
using HybridDb.Migrations.Commands;
using Serilog;

namespace HybridDb.Migrations
{
    public class SchemaMigrationRunner
    {
        readonly ILogger logger;
        readonly IDocumentStore store;
        readonly IReadOnlyList<Migration> migrations;
        readonly ISchemaDiffer differ;

        public SchemaMigrationRunner(IDocumentStore store, ISchemaDiffer differ)
        {
            this.store = store;
            this.differ = differ;
            
            logger = store.Configuration.Logger;
            migrations = store.Configuration.Migrations;
        }

        public void Run()
        {
            var requiresReprojection = new List<string>();

            var database = ((DocumentStore)store).Database;
            var configuration = store.Configuration;

            using (var tx = new TransactionScope(TransactionScopeOption.RequiresNew, new TransactionOptions { IsolationLevel = IsolationLevel.Serializable }))
            {
                var metadata = new Table("HybridDb", new Column("SchemaVersion", typeof(int)));
                configuration.Tables.TryAdd(metadata.Name, metadata);

                new CreateTable(metadata).Execute(database);

                int currentSchemaVersion = database.RawQuery<int>(
                    string.Format("select top 1 SchemaVersion from {0} with (tablockx, holdlock)", 
                        database.FormatTableName("HybridDb"))).SingleOrDefault();

                if (currentSchemaVersion > store.Configuration.ConfiguredVersion)
                {
                    throw new InvalidOperationException(string.Format(
                        "Database schema is ahead of configuration. Schema is version {0}, but configuration is version {1}.", 
                        currentSchemaVersion, store.Configuration.ConfiguredVersion));
                }

                if (database.TableMode == TableMode.UseRealTables)
                {
                    if (currentSchemaVersion < configuration.ConfiguredVersion)
                    {
                        var migrationsToRun = migrations.OrderBy(x => x.Version).Where(x => x.Version > currentSchemaVersion).ToList();
                        logger.Information("Migrates schema from version {0} to {1}.", currentSchemaVersion, configuration.ConfiguredVersion);

                        foreach (var migration in migrationsToRun)
                        {
                            var migrationCommands = migration.MigrateSchema();
                            foreach (var command in migrationCommands)
                            {
                                requiresReprojection.AddRange(ExecuteCommand(database, command));
                            }

                            currentSchemaVersion++;
                        }
                    }
                }
                else
                {
                    logger.Information("Skips provided migrations when not using real tables.");
                }

                var schema = currentSchemaVersion > 0
                    ? database.QuerySchema().Values.ToList()
                    : new List<Table>();

                var commands = differ.CalculateSchemaChanges(schema, configuration);

                if (commands.Any())
                {
                    logger.Information("Found {0} differences between current schema and configuration. Migrates schema to get up to date.", commands.Count);

                    foreach (var command in commands)
                    {
                        requiresReprojection.AddRange(ExecuteCommand(database, command));
                    }
                }

                foreach (var tablename in requiresReprojection)
                {
                    // TODO: Only set RequireReprojection on command if it is documenttable - can it be done?
                    var design = configuration.DocumentDesigns.FirstOrDefault(x => x.Table.Name == tablename);
                    if (design == null) continue;

                    database.RawExecute(string.Format("update {0} set AwaitsReprojection=@AwaitsReprojection",
                        database.FormatTableNameAndEscape(tablename)), new { AwaitsReprojection = true });
                }

                database.RawExecute(string.Format(@"
if not exists (select * from {0}) 
    insert into {0} (SchemaVersion) values (@version); 
else
    update {0} set SchemaVersion=@version",
                    database.FormatTableName("HybridDb")),
                    new { version = currentSchemaVersion });

                tx.Complete();
            }
        }

        IEnumerable<string> ExecuteCommand(Database database, SchemaMigrationCommand command)
        {
            if (command.Unsafe)
            {
                logger.Warning("Unsafe migration command '{0}' was skipped.", command.ToString());
                yield break;
            }

            logger.Information("Executing migration command '{0}'.", command.ToString());

            command.Execute(database);

            if (command.RequiresReprojectionOf != null)
                yield return command.RequiresReprojectionOf;
        }
    }
}