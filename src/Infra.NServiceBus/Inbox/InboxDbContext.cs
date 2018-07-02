using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.Entity;
using System;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Linq;

namespace Infra.NServiceBus.Inbox
{
    public class InboxDbContext : DbContext
    {
        protected string dbSchemaName;
        protected string dbTablePrefix;

        public InboxDbContext()
        {
            TurnOffAutomaticDatabaseCreationAndSchemaUpdates();
            dbSchemaName = ConfigurationManager.AppSettings["DatabaseSchemaName"];
            dbTablePrefix = ConfigurationManager.AppSettings["EndpointTablePrefix"];
        }

        public InboxDbContext(IDbConnection connection)
            : base((DbConnection)connection, false)
        {
            TurnOffAutomaticDatabaseCreationAndSchemaUpdates();
            dbSchemaName = ConfigurationManager.AppSettings["DatabaseSchemaName"];
            dbTablePrefix = ConfigurationManager.AppSettings["EndpointTablePrefix"];
        }

        public DbSet<InboxRecord> InboxRecords { get; set; }
       
        protected override void OnModelCreating(DbModelBuilder modelBuilder)
        {
          
            if (string.IsNullOrEmpty(dbSchemaName))
            {
                throw new Exception("Missing schema name in app.config. Please add value for DatabaseSchemaName");
            }
           
            modelBuilder.Types().Configure(c => c.ToTable(dbTablePrefix + c.ClrType.Name, dbSchemaName));
            base.OnModelCreating(modelBuilder);
        }

        private static void TurnOffAutomaticDatabaseCreationAndSchemaUpdates()
        {
            Database.SetInitializer<InboxDbContext>(null);
        }

        public async Task PersistHandledMessage(InboxRecord inboxMessage)
        {
            const string sql = @"MERGE INTO [{0}].[{1}InboxRecord] WITH (updlock, rowlock) AS tgt
                        USING
                          (SELECT @ContentId, @CheckMessageOrderType) AS src (ContentId, CheckMessageOrderType)
                          ON tgt.ContentId = src.ContentId and tgt.CheckMessageOrderType = src.CheckMessageOrderType 
                        WHEN MATCHED THEN
                          UPDATE       
                            SET 
                                ContentVersion = @ContentVersion, 
                                ContentId = @ContentId, 
                                ModifiedAtUtc = @ModifiedAtUtc,
                                MessageId = @MessageId 
                        WHEN NOT MATCHED THEN
                          INSERT (ContentId, ContentVersion, CheckMessageOrderType, ModifiedAtUtc, MessageId) 
                          VALUES (@ContentId, @ContentVersion, @CheckMessageOrderType, @ModifiedAtUtc, @MessageId);";

            object[] parameters = { dbSchemaName, dbTablePrefix };
            await this.Database.ExecuteSqlCommandAsync(
                string.Format(sql, parameters),
                new SqlParameter("@ContentId", inboxMessage.ContentId),
                new SqlParameter("@ContentVersion", inboxMessage.ContentVersion),
                new SqlParameter("@CheckMessageOrderType", inboxMessage.CheckMessageOrderType),
                new SqlParameter("@ModifiedAtUtc", inboxMessage.ModifiedAtUtc),
                new SqlParameter("@MessageId", inboxMessage.MessageId))
                .ConfigureAwait(false);
        }

        public async Task PersistDiscardedMessage(InboxRecord inboxMessage, long latestHandledVersion)
        {
            //Add or update
            const string sql = @"INSERT INTO [{0}].[{1}InboxRecordDiscarded] (ContentId, ContentVersion, LatestContentVersion, CheckMessageOrderType, ModifiedAtUtc, MessageId)
		                        VALUES (@ContentId, @ContentVersion, @LatestContentVersion, @CheckMessageOrderType, @ModifiedAtUtc, @MessageId)";

            object[] parameters = { dbSchemaName, dbTablePrefix };
            await this.Database.ExecuteSqlCommandAsync(
                  string.Format(sql, parameters),
                new SqlParameter("@ContentId", inboxMessage.ContentId),
                new SqlParameter("@ContentVersion", inboxMessage.ContentVersion),
                new SqlParameter("@LatestContentVersion", latestHandledVersion),
                new SqlParameter("@CheckMessageOrderType", inboxMessage.CheckMessageOrderType),
                new SqlParameter("@ModifiedAtUtc", inboxMessage.ModifiedAtUtc),
                new SqlParameter("@MessageId", inboxMessage.MessageId))
                .ConfigureAwait(false);
        }

        public async Task<long?> GetLatestMessageVersion(string contentId, string checkMessageOrderType)
        {
            return await InboxRecords
                    .Where(x => x.ContentId == contentId && x.CheckMessageOrderType == checkMessageOrderType)
                    .Select(x => x.ContentVersion)
                    .SingleOrDefaultAsync();
        }
    }
}
