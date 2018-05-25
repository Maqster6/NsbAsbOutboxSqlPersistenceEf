using System;
using System.Threading.Tasks;
using Domain;
using Endpoint2;
using Infra.NServiceBus;
using NServiceBus;


class Program
{
    public static async Task Main()
    {
        var endpointConfiguration = new EndpointConfiguration("Endpoint2");
        endpointConfiguration.DefineEndpointName("Endpoint2");
        endpointConfiguration.EnableOutbox();
        endpointConfiguration.EnablePersistAndPublish<OrderDbContext>();
        endpointConfiguration.RegisterComponents(c=>c.ConfigureComponent<OrderStorageContext>(DependencyLifecycle.SingleInstance));
        var instance = await Endpoint.Start(endpointConfiguration)
            .ConfigureAwait(false);
        await Task.Delay(-1)
            .ConfigureAwait(false);
        // Unreachable
        await instance.Stop()
            .ConfigureAwait(false);
    }
}