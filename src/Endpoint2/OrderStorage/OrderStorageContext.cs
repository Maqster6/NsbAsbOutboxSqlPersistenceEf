using System;
using Domain;
using NServiceBus;

namespace Endpoint2
{
    /// <summary>
    /// This only exist to warp the extension method as these cannot easily to mocked for unit testing.
    /// </summary>
    class OrderStorageContext : IOrderStorageContext
    {
        public OrderDbContext GetOrderDbContext(IMessageHandlerContext context)
        {
            if (!context.Extensions.TryGet(out OrderDbContext dataContext)) throw new Exception($"No dbcontext set for '{typeof(OrderDbContext)}.");
            return dataContext;
        }
    }
}