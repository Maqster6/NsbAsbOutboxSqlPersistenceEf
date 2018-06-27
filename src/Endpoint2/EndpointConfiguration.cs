using System;
using System.Threading.Tasks;
using Domain;
using Endpoint2;
using Infra.NServiceBus;
using Infra.NServiceBus.Inbox;
using NServiceBus;


class Program
{
    public static async Task Main()
    {
        var endpointConfiguration = new EndpointConfiguration("Endpoint2");
        endpointConfiguration.DefineEndpointName("Endpoint2");
        endpointConfiguration.EnableOutbox();
        endpointConfiguration.EnableInbox();
        endpointConfiguration.EnablePersistAndPublish<OrderDbContext>();
        
        var instance = await Endpoint.Start(endpointConfiguration)
            .ConfigureAwait(false);
        await Task.Delay(-1)
            .ConfigureAwait(false);
        // Unreachable
        await instance.Stop()
            .ConfigureAwait(false);
    }
}