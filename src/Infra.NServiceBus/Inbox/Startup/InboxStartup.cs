using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Azure;
using NServiceBus.Features;

namespace Infra.NServiceBus.Inbox.Startup
{
    public class InboxStartup : Feature
    {
        protected override void Setup(FeatureConfigurationContext context)
        {
            using (var startupDbContext = new InboxStartupDbContext())
            {
                startupDbContext.InitializeDatabase();
            }

            context.RegisterStartupTask(new InboxCleanupTask());
        }
    }
}
