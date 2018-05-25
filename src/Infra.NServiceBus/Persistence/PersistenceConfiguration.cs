using System;
using System.Data.SqlClient;
using Microsoft.Azure;
using NServiceBus;
using NServiceBus.Persistence.Sql;

namespace Infra.NServiceBus.Persistence
{
    public class PersistenceConfiguration : INeedInitialization
    {
        public void Customize(EndpointConfiguration configuration)
        {
            var connectionString = Environment.GetEnvironmentVariable("AzureDb.ConnectionString");

            if (string.IsNullOrEmpty(connectionString))
            {
                throw new Exception("Database connectionstring is not set");
            }
            configuration.EnableInstallers();
            var persistence = configuration.UsePersistence<SqlPersistence>();
            persistence.SubscriptionSettings().DisableCache();
            persistence.TablePrefix("Endpoint_");
            var dialect = persistence.SqlDialect<SqlDialect.MsSqlServer>();
            dialect.Schema("OrderContext");
            persistence.ConnectionBuilder(
                connectionBuilder: () => new SqlConnection(connectionString));
        }
    }
}
