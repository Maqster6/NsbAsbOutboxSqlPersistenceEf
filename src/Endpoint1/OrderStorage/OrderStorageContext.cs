using Domain;
using NServiceBus;
using NServiceBus.Persistence;

namespace Endpoint1
{
    public class OrderStorageContext : IOrderStorageContext
    {
        public OrderDbContext GetOrderDbContext(SynchronizedStorageSession session)
        {
            var sqlPersistenceSession = session.SqlPersistenceSession();
            var context = new OrderDbContext(sqlPersistenceSession.Connection);
            context.Database.UseTransaction(sqlPersistenceSession.Transaction);
            return context;
        }
    }
}