using System.Data.Entity.Migrations;

namespace Infra.NServiceBus.Inbox
{
    internal sealed class InboxDbContextConfiguration : DbMigrationsConfiguration<InboxDbContext>
    {
        public InboxDbContextConfiguration()
        {
            AutomaticMigrationsEnabled = false;
        }
    }
}
