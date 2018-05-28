using System.Threading.Tasks;
using Domain;
using Endpoint2.Commands;
using Infra.NServiceBus.Persistence;
using NServiceBus;
using NServiceBus.Logging;

namespace Endpoint2
{
    public class PlaceOrderCommandHandler : IHandleMessages<PlaceOrderCommand>
    {
        static ILog log = LogManager.GetLogger<PlaceOrderCommand>();
        readonly IDbContextWrapper<OrderDbContext> dbContextWrapper;

        public PlaceOrderCommandHandler(IDbContextWrapper<OrderDbContext> dbContextWrapper)
        {
            this.dbContextWrapper = dbContextWrapper;
        }

        public Task Handle(PlaceOrderCommand placeOrderCommand, IMessageHandlerContext context)
        {
            log.Info($"Endpoint2 Received PlaceOrderCommand: {placeOrderCommand.OrderNumber}");

            var dataContext = dbContextWrapper.GetDbContext(context);

            var order = Domain.Order.Create(placeOrderCommand.OrderId, placeOrderCommand.OrderNumber);
            order.PlaceOrder(placeOrderCommand.PlacedAtDate);
            dataContext.Orders.Add(order);
            return  Task.CompletedTask;
        }
    }
}

