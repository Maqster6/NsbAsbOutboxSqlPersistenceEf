using Domain;
using NServiceBus.Persistence;

namespace Endpoint1
{
    public interface IOrderStorageContext
    {
        OrderDbContext GetOrderDbContext(SynchronizedStorageSession session);
    }
}