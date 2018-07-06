using System.Data.Entity;
using Infra.NServiceBus.Inbox;
using Infra.NServiceBus.Inbox.Startup;
using Infra.NServiceBus.Persistence;
using NServiceBus;

namespace Infra.NServiceBus
{
    public static class EndpointConfigurationExtensions
    {
        public static void EnablePersistAndPublish<TDbContext>(this EndpointConfiguration configuration)
           where TDbContext : DbContext
        {
            var pipeline = configuration.Pipeline;
            pipeline.Register(
                behavior: new PersistAndPublishBehavior<TDbContext>(),
                description: "Persist data in db and raise events if any");

            configuration.RegisterComponents(
                components =>
                {
                    components.ConfigureComponent<PersistAndPublishBehavior<TDbContext>>(DependencyLifecycle.InstancePerUnitOfWork);
                    components.ConfigureComponent<DbContextWrapper<TDbContext>>(DependencyLifecycle.SingleInstance);
                });
        }

        public static void EnableInbox(this EndpointConfiguration configuration)
        {
            var pipeline = configuration.Pipeline;
            pipeline.Register(
                behavior: new InboxBehavior<InboxDbContext>(),
                description: "Idempotency");

            configuration.RegisterComponents(
                components =>
                {
                    components.ConfigureComponent<InboxBehavior<InboxDbContext>>(DependencyLifecycle.InstancePerUnitOfWork);
                    components.ConfigureComponent<DbContextWrapper<InboxDbContext>>(DependencyLifecycle.SingleInstance);
                });
            configuration.EnableFeature<InboxStartup>();
        }
    }
}
