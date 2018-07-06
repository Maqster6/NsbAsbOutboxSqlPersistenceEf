using System;
using System.Configuration;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using NServiceBus;
using NServiceBus.Features;
using NServiceBus.Logging;

namespace Infra.NServiceBus.Inbox.Startup
{
    public sealed class InboxCleanupTask : FeatureStartupTask, IDisposable
    {
        static ILog log = LogManager.GetLogger<InboxCleanupTask>();
        private Timer cleanupTimer;
        private readonly TimeSpan retentionPeriod;

        public InboxCleanupTask()
        {
            string retentionPeriodValue = ConfigurationManager.AppSettings["InboxRetentionPeriodTimespan"];
            if (string.IsNullOrEmpty(retentionPeriodValue))
            {
                log.Info("InboxRetentionPeriodTimespan appsetting key not found. Setting to default value of 7 days");
                retentionPeriod = new TimeSpan(7,0,0,0);
            }
            else
            {
                retentionPeriod = TimeSpan.Parse(retentionPeriodValue, CultureInfo.InvariantCulture);
            }
        }

        public void Dispose()
        {
            cleanupTimer.Dispose();
        }

        protected override Task OnStart(IMessageSession session)
        {
            cleanupTimer = new Timer(PerformCleanup, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
            return Task.CompletedTask;
        }

        protected override Task OnStop(IMessageSession session)
        {
            using (var waitHandle = new ManualResetEvent(false))
            {
                cleanupTimer.Dispose(waitHandle);

                waitHandle.WaitOne();
            }
            return Task.CompletedTask;
        }

        private void PerformCleanup(object state)
        {
            try
            {
                using (var startupDbContext = new InboxStartupDbContext())
                {
                    var time = DateTime.UtcNow - retentionPeriod;
                    startupDbContext.RemoveEntriesOlderThan(time);
                    log.Info($"Performing Inbox Cleanup for items older than {time}");
                }
            }
            catch (Exception exception)
            {
                log.Error("Error when trying to remove old entries from Inbox", exception);
            }
        }
    }
}