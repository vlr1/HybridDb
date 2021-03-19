using System;
using System.Data.SqlClient;
using Dapper;

namespace HybridDb.Queue
{
    public class EnqueueCommand : Command<string>
    {
        public EnqueueCommand(QueueTable table, HybridDbMessage message, string topic = "messages")
        {
            Table = table;
            Message = message;
            Topic = topic;
        }

        public QueueTable Table { get; }
        public string Topic { get; }
        public HybridDbMessage Message { get; }

        public static string Execute(Func<object, string> serializer, DocumentTransaction tx, EnqueueCommand command)
        {
            var tablename = tx.Store.Database.FormatTableNameAndEscape(command.Table.Name);
            
            var discriminator = tx.Store.Configuration.TypeMapper.ToDiscriminator(command.Message.GetType());

            try
            {
                tx.SqlConnection.Execute(@$"
                    set nocount on; 
                    insert into {tablename} (Topic, Id, CommitId, Discriminator, Message) 
                    values (@Topic, @Id, @CommitId, @Discriminator, @Message); 
                    set nocount off;",
                    new
                    {
                        command.Topic,
                        command.Message.Id,
                        tx.CommitId,
                        Discriminator = discriminator,
                        Message = serializer(command.Message)
                    },
                    tx.SqlTransaction);
            }
            catch (SqlException e)
            {
                // Enqueuing is idempotent. It should ignore exceptions from primary key violations and just not insert the message.
                if (e.Number == 2627) return command.Message.Id;

                throw;
            }

            return command.Message.Id;
        }
    }
}