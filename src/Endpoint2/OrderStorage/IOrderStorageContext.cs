using Domain;
using NServiceBus;

namespace Endpoint2
{
    public interface IOrderStorageContext
    {
        OrderDbContext GetOrderDbContext(IMessageHandlerContext session);
    }
}