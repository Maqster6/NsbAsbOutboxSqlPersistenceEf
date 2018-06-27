using NServiceBus;
using NServiceBus.Features;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Infra.NServiceBus.Inbox
{
    class InboxStartup : Feature
    {
        protected override void Setup(FeatureConfigurationContext context)
        {
            context.Container.ConfigureComponent<InboxStartupTask>(DependencyLifecycle.SingleInstance)
                             .ConfigureProperty(t => t.TimeToKeepDeduplicationData, GetTimeToKeepDeduplicationData(context));
        }

        private static TimeSpan GetTimeToKeepDeduplicationData(FeatureConfigurationContext context)
        {
            TimeSpan dt;
            return context.Settings.TryGet("Outbox.TimeToKeepDeduplicationEntries", out dt) ? dt : TimeSpan.FromDays(7);
        }

        private class InboxStartupTask : FeatureStartupTask
        {
            protected override Task OnStart(IMessageSession session)
            {
                throw new NotImplementedException();
            }

            protected override Task OnStop(IMessageSession session)
            {
                throw new NotImplementedException();
            }
        }
    }
}
