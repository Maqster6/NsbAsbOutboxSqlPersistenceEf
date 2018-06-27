using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.Entity;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Infra.NServiceBus.Inbox
{
    public class InboxRepository 
    {
        private readonly InboxDbContext dbContext;
        private readonly string dbSchemaName;

        public InboxRepository(InboxDbContext dbContext)
        {
            this.dbContext = dbContext;
            dbSchemaName = ConfigurationManager.AppSettings["DatabaseSchemaName"];
        }

        public async Task PersistHandledMessage(InboxMessage inboxMessage)
        {
            const string sql = @"MERGE INTO [{0}].InboxRecord WITH (updlock, rowlock) AS tgt
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

            await dbContext.Database.ExecuteSqlCommandAsync(
                string.Format(sql, dbSchemaName),
                new SqlParameter("@ContentId", inboxMessage.ContentId),
                new SqlParameter("@ContentVersion", inboxMessage.ContentVersion),
                new SqlParameter("@CheckMessageOrderType", inboxMessage.CheckMessageOrderType),
                new SqlParameter("@ModifiedAtUtc", inboxMessage.ModifiedAtUtc),
                new SqlParameter("@MessageId", inboxMessage.MessageId))
                .ConfigureAwait(false);
        }

        public async Task PersistDiscardedMessage(InboxMessage inboxMessage, long latestHandledVersion)
        {
            //Add or update
            const string sql = @"INSERT INTO [{0}].[InboxRecordDiscarded] (ContentId, ContentVersion, LatestContentVersion, CheckMessageOrderType, ModifiedAtUtc, MessageId)
		                        VALUES (@ContentId, @ContentVersion, @LatestContentVersion, @CheckMessageOrderType, @ModifiedAtUtc, @MessageId)";

            await dbContext.Database.ExecuteSqlCommandAsync(
                string.Format(sql, dbSchemaName),
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
            return await dbContext.InboxRecords
                    .Where(x => x.ContentId == contentId && x.CheckMessageOrderType == checkMessageOrderType)
                    .Select(x => x.ContentVersion)
                    .SingleOrDefaultAsync();
        }
    }
}
