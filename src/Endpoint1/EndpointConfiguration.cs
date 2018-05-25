using Domain;
using Infra.NServiceBus;
using NServiceBus;

namespace Endpoint1
{
    public partial class EndpointConfig : IConfigureThisEndpoint, AsA_Worker
    {
        public void Customize(EndpointConfiguration endpointConfiguration)
        {
            endpointConfiguration.DefineEndpointName("Endpoint1");
            endpointConfiguration.EnableOutbox();
            endpointConfiguration.RegisterComponents(c =>
            {
                c.ConfigureComponent<OrderStorageContext>(DependencyLifecycle.InstancePerUnitOfWork);
            });                 
            endpointConfiguration.EnablePersistAndPublish<OrderDbContext>();
        }
    }
}

