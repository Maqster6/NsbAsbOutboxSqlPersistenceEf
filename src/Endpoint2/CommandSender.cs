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
            public async Task Start(IMessageSession session)
            {
                // Not return task, forking a new thread!
#pragma warning disable 4014
                Task.Run(() => Loop(session));
#pragma warning restore 4014
            }

            async void Loop(IMessageSession session)
            {
                Console.WriteLine("Press enter to send Place Order Command");
                int tempOrderNumber = 0;
                string tempContentId = string.Empty;
                string tempContentVersion = "0";

                while (true)
                {
                    var key = Console.ReadKey();

                    if (key.Key == ConsoleKey.Enter)
                    {
                        for (int i = 0; i < 2000; i++)
                        {
                            var orderNumber = i + 1;
                            Guid orderId = Guid.NewGuid();
                            string contentId = orderId.ToString();
                            string contentVersion = DateTime.UtcNow.Ticks.ToString(); //(i + 1).ToString();

                            if (i % 2 == 1) // DiscardedMessages 
                            {
                                orderNumber = tempOrderNumber;
                                contentId = tempContentId;
                                contentVersion = tempContentVersion;//"-1";
                            }
                            var placeOrderCommand = new PlaceOrderCommand
                            {
                                OrderId = orderId,
                                OrderNumber = orderNumber,
                                PlacedAtDate = DateTime.UtcNow
                            };
                            var sendOptions = new SendOptions();

                            sendOptions.SetHeader("ContentId", contentId);
                            sendOptions.SetHeader("ContentVersion", contentVersion);
                            sendOptions.SetDestination("LoadTest");
                            await session.Send(placeOrderCommand, sendOptions)
                                         .ConfigureAwait(false);

                            tempOrderNumber = orderNumber;
                            tempContentId = contentId;
                            tempContentVersion = (long.Parse(contentVersion) - 1000).ToString();

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

