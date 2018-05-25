using Domain;
using NServiceBus.Persistence;

namespace Endpoint2
{
    public interface IOrderStorageContext
    {
        OrderDbContext GetOrderDbContext(SynchronizedStorageSession session);
    }
}