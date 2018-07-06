using NServiceBus;
using System;
using System.Data;

namespace Infra.NServiceBus.Transport
{
    public static class TransportConfigurationExtensions
    {
        public static TransportExtensions<AzureServiceBusTransport> SetTransport(this EndpointConfiguration configuration)
        {
            var transport = configuration.UseTransport<AzureServiceBusTransport>();
            var connectionString = Environment.GetEnvironmentVariable("AzureServiceBus.ConnectionString");
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new Exception("Azure Service Bus ConnectionString is missing");
            }

            transport.ConnectionString(connectionString);

            transport.Transactions(TransportTransactionMode.ReceiveOnly);
            var queues = transport.Queues();
            //queues.EnablePartitioning(true);
            queues.MaxDeliveryCount(12);
            transport.Subscriptions().MaxDeliveryCount(12);

            //lower concurrency if sending more message per receive
            var perReceiverConcurrency = 16;

            // increase number of receivers as much as bandwidth allows (probably less than receiver due to send volume)
            var numberOfReceivers = 4;

            var globalConcurrency = numberOfReceivers * perReceiverConcurrency;

            configuration.LimitMessageProcessingConcurrencyTo(globalConcurrency);
            var receivers = transport.MessageReceivers();
            receivers.PrefetchCount(perReceiverConcurrency);

            var factories = transport.MessagingFactories();
            factories.NumberOfMessagingFactoriesPerNamespace(numberOfReceivers * 2);
            transport.NumberOfClientsPerEntity(numberOfReceivers);

            transport.MessageSenders().MaximuMessageSizeInKilobytes(1024);
            transport.Sanitization().UseStrategy<SanitizationStrategy>();
            return transport;
        }

        public static AzureServiceBusEndpointOrientedTopologySettings SetTopology(this EndpointConfiguration configuration, TransportExtensions<AzureServiceBusTransport> transport)
        {
            var topology = transport.UseEndpointOrientedTopology();
            return topology;
        }
    }
}
