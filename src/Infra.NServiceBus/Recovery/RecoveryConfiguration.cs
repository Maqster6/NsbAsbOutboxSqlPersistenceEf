using System;
using NServiceBus;

namespace Infra.NServiceBus.Recovery
{
    public class RecoveryConfiguration : INeedInitialization
    {
        public void Customize(EndpointConfiguration configuration)
        {
            var recoverability = configuration.Recoverability();
            recoverability.DisableLegacyRetriesSatellite();            
            recoverability.Immediate(
                customizations: immediate =>
                {
                    immediate.NumberOfRetries(0);
                });
            recoverability.Delayed(
                customizations: delayed =>
                {
                    var numberOfRetries = delayed.NumberOfRetries(0);
                    numberOfRetries.TimeIncrease(TimeSpan.FromSeconds(30));
                });
        }
    }
}
