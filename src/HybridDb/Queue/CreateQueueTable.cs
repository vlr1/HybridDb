﻿using HybridDb.Migrations.Schema;

namespace HybridDb.Queue
{
    public class CreateQueueTable : DdlCommand
    {
        public QueueTable QueueTable { get; }

        public CreateQueueTable(QueueTable queueTable)
        {
            Safe = true;

            QueueTable = queueTable;
        }

        public override string ToString() => "Create queue table";

        public override void Execute(DocumentStore store)
        {
            var tableName = store.Database.FormatTableName(QueueTable.Name);

            store.Database.RawExecute($@"
                if (object_id('{tableName}', 'U') is null)
                begin
                    CREATE TABLE [dbo].[{tableName}] (
                        [Topic] [nvarchar](850) NOT NULL,
                        [Version] [nvarchar](40) NOT NULL,
	                    [Id] [nvarchar](850) NOT NULL,
	                    [CommitId] [uniqueidentifier] NOT NULL,
	                    [Discriminator] [nvarchar](850) NOT NULL,
	                    [Message] [nvarchar](max) NULL,

                        CONSTRAINT [PK_{tableName}] PRIMARY KEY CLUSTERED ([Topic] ASC, [Id] ASC)
                    )
                end", schema: true);
        }
    }
}