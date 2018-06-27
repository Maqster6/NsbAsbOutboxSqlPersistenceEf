using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Infra.NServiceBus.Inbox
{
    public interface IInboxRepository
    {
        void PersistHandledMessage(InboxMessage inboxMessage);
        void PersistDiscardedMessage(InboxMessage inboxMessage, long latestHandledVersion);
        long? GetLatestMessageVersion(string contentId, string checkMessageOrderType);
    }
}
