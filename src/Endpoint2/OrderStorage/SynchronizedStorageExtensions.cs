using Domain;
using NServiceBus;
using NServiceBus.Persistence;

namespace Endpoint2
{
    public static class SynchronizedStorageExtensions
    {
        public static OrderDbContext FromCurrentSession(SynchronizedStorageSession session)      
        {
            var sqlPersistenceSession = session.SqlPersistenceSession();
            var context = new OrderDbContext(sqlPersistenceSession.Connection);
            context.Database.UseTransaction(sqlPersistenceSession.Transaction);
            return context;
        }
    }
}

