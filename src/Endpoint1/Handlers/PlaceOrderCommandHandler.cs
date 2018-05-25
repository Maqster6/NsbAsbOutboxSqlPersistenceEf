using System.Threading.Tasks;
using Endpoint1.Commands;
using Infra.NServiceBus.Persistence;
using NServiceBus;
using NServiceBus.Logging;

namespace Endpoint1
{
    public class PlaceOrderCommandHandler : IHandleMessages<PlaceOrderCommand>
    {
        private readonly IOrderStorageContext orderStorageContext;
        static ILog log = LogManager.GetLogger<PlaceOrderCommand>();

        public PlaceOrderCommandHandler(IOrderStorageContext orderStorageContext)
        {
            this.orderStorageContext = orderStorageContext;
        }

        public async Task Handle(PlaceOrderCommand placeOrderCommand, IMessageHandlerContext context)
        {
            log.Info($"Endpoint1 Received PlaceOrderCommand: {placeOrderCommand.OrderNumber}");


            using (var dataContext = orderStorageContext.GetOrderDbContext(context.SynchronizedStorageSession))
            {
                var order = Domain.Order.Create(placeOrderCommand.OrderId, placeOrderCommand.OrderNumber);
                order.PlaceOrder(placeOrderCommand.PlacedAtDate);
                dataContext.Orders.Add(order);
                await DataContextPersistence.SaveChangesAsync(dataContext)
                    .ConfigureAwait(false);
            }
        }
    }
}

