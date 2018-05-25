using NServiceBus;

namespace Infra.NServiceBus.Transport
{
    public class TransportConfiguration : INeedInitialization
    {
        public void Customize(EndpointConfiguration endpointConfiguration)
        {
            var transport = endpointConfiguration.UseTransport<MsmqTransport>();
        }
    }
}
