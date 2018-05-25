using System.Threading.Tasks;
using Endpoint2.Commands;
using NServiceBus;
using NServiceBus.Logging;

namespace Endpoint2
{
    public class PlaceOrderCommandHandler : IHandleMessages<PlaceOrderCommand>
    {
        static ILog log = LogManager.GetLogger<PlaceOrderCommand>();
        readonly IOrderStorageContext orderStorageContext;

        public PlaceOrderCommandHandler(IOrderStorageContext orderStorageContext)
        {
            this.orderStorageContext = orderStorageContext;
        }

        public Task Handle(PlaceOrderCommand placeOrderCommand, IMessageHandlerContext context)
        {
            log.Info($"Endpoint2 Received PlaceOrderCommand: {placeOrderCommand.OrderNumber}");

            var dataContext = orderStorageContext.GetOrderDbContext(context.SynchronizedStorageSession);

            var order = Domain.Order.Create(placeOrderCommand.OrderId, placeOrderCommand.OrderNumber);
            order.PlaceOrder(placeOrderCommand.PlacedAtDate);
            dataContext.Orders.Add(order);
            return  Task.CompletedTask;
        }
    }
}

