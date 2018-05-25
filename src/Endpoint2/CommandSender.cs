using System;
using System.Threading.Tasks;
using Endpoint2.Commands;
using NServiceBus;

namespace Endpoint2
{
    public partial class EndpointConfig
    {
        public class CommandSender : IWantToRunWhenEndpointStartsAndStops
        {
            public  Task Start(IMessageSession session)
            {
                return Task.Run(() => Loop(session));
            }

            async void Loop(IMessageSession session)
            {
                Console.WriteLine("Press enter to send Place Order Command");

                while (true)
                {
                    var key = Console.ReadKey();

                    if (key.Key == ConsoleKey.Enter)
                    {
                        for (int i = 0; i < 10; i++)
                        {
                            var orderNumber = i + 1;
                            await session.Send("Endpoint2",
                                new PlaceOrderCommand
                                {
                                    OrderId = Guid.NewGuid(),
                                    OrderNumber = orderNumber,
                                    PlacedAtDate = DateTime.UtcNow
                                })
                                .ConfigureAwait(false);
                            Console.WriteLine($"Place Order Command sent {orderNumber}");
                        }

                    }
                }
            }

            public Task Stop(IMessageSession session)
            {
                return Task.CompletedTask;
            }
        }
    }
}

