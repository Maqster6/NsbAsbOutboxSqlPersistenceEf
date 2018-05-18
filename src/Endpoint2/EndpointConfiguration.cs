﻿using System;
using System.Threading.Tasks;
using Domain;
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
        var instance = await Endpoint.Start(endpointConfiguration);
        await Task.Delay(-1);
        await instance.Stop();
    }
}