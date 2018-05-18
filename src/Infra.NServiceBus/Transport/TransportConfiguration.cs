﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NServiceBus;

namespace Infra.NServiceBus.Transport
{
    public class TransportConfiguration : INeedInitialization
    {
        public void Customize(EndpointConfiguration endpointConfiguration)
        {
            var transport = endpointConfiguration.UseTransport<AzureServiceBusTransport>();
            var connectionString = Environment.GetEnvironmentVariable("AzureServiceBus.ConnectionString");
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new Exception("Azure Service Bus ConnectionString is missing");
            }
            transport.ConnectionString(connectionString);
            transport.Sanitization().UseStrategy<SanitizationStrategy>();
            transport.UseEndpointOrientedTopology();
        }
    }
}