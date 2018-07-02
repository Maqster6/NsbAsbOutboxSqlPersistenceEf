using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NServiceBus;
using NServiceBus.Logging;
using NServiceBus.Pipeline;

namespace Infra.NServiceBus.Inbox
{
    public class InboxBehavior<TDbContext> : Behavior<IInvokeHandlerContext> where TDbContext : InboxDbContext
    {
        static ILog log = LogManager.GetLogger<InboxBehavior<TDbContext>>();
        public override async Task Invoke(IInvokeHandlerContext context, Func<Task> next)
        {
            var session = context.SynchronizedStorageSession;
            var sqlPersistenceSession = session.SqlPersistenceSession();
            var dbContext = (InboxDbContext)Activator.CreateInstance(typeof(InboxDbContext), sqlPersistenceSession.Connection);

            using (dbContext)
            {
                dbContext.Database.UseTransaction(sqlPersistenceSession.Transaction);
 
                context.MessageHeaders.TryGetValue("ContentId", out string contentId);
                long contentVersion = GetContentVersionFrom(context.MessageHeaders);

                var checkMessageOrderType = GetCheckMessageOrderType(context);

                var latestHandledVersion = await dbContext.GetLatestMessageVersion(contentId, checkMessageOrderType).ConfigureAwait(false);

                var shouldBeHandled = latestHandledVersion == null || contentVersion > latestHandledVersion;

                var inboxMessage = new InboxRecord
                {
                    ContentId = contentId,
                    ContentVersion = contentVersion,
                    CheckMessageOrderType = checkMessageOrderType,
                    MessageId = Guid.Parse(context.MessageId),
                    ModifiedAtUtc = DateTime.UtcNow
                };

                if (shouldBeHandled)
                {
                    log.Info($"Handling message: Type: {checkMessageOrderType}, ContentId: {contentId}, ContentVersion: {contentVersion}");
                    await dbContext.PersistHandledMessage(inboxMessage).ConfigureAwait(false);
                    await next().ConfigureAwait(false);
                }
                else
                {
                    string discardedReason = $"Discarding message since a newer version has already been processed: Type: {checkMessageOrderType}, ContentId: {contentId}, ContentVersion: {contentVersion}";
                    log.Info(discardedReason);
                    await dbContext.PersistDiscardedMessage(inboxMessage, latestHandledVersion.GetValueOrDefault()).ConfigureAwait(false);
                    context.Headers.Add("InboxDiscardedReason", discardedReason);
                }
            }
        }

        private static long GetContentVersionFrom(IReadOnlyDictionary<string, string> messageHeaders)
        {
            messageHeaders.TryGetValue("ContentVersion", out string contentVersionHeaderValue);
            var successFullyParsed = long.TryParse(contentVersionHeaderValue, out long contentVersion);
            return successFullyParsed?contentVersion : throw new Exception($"Could not parse content version to long. Value was {contentVersionHeaderValue}");
        }

        private static string GetCheckMessageOrderType(IInvokeHandlerContext context)
        {
            context.MessageHeaders.TryGetValue("CheckMessageOrderType", out string checkMessageOrderType);
            if (!string.IsNullOrEmpty(checkMessageOrderType))
            {
                return checkMessageOrderType;
            }

            var messageType = context.MessageMetadata.MessageType;
            var checkMessageOrder = ChecksMessageOrderBy(messageType, typeof(ICheckMessageOrder<>));
            return checkMessageOrder == null ? messageType.FullName : checkMessageOrder.GetGenericArguments()[0].FullName;
        }

        private static Type ChecksMessageOrderBy(Type typeToExamine, Type genericInterfaceToLookFor)
        {
            var @interface = typeToExamine.GetInterfaces().SingleOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == genericInterfaceToLookFor);
            return @interface;
        }
    }
}
