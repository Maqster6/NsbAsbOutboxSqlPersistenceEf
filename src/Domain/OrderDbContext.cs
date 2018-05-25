using System.Data;
using System.Data.Common;
using System.Data.Entity;
using System.Data.Entity.ModelConfiguration.Conventions;

namespace Domain
{
    public class OrderDbContext : DbContext
    {
        public OrderDbContext()
        {
        }

        public OrderDbContext(IDbConnection connection) : base((DbConnection)connection, false)
        {
            TurnOffAutomaticDatabaseCreationAndSchemaUpdates();
        }

        public DbSet<Order> Orders { get; set; }

        protected override void OnModelCreating(DbModelBuilder modelBuilder)
        {
            //modelBuilder.HasDefaultSchema("OrderContext");
            modelBuilder.Conventions.Remove<PluralizingTableNameConvention>();

            base.OnModelCreating(modelBuilder);
        }
        private void TurnOffAutomaticDatabaseCreationAndSchemaUpdates()
        {
            Database.SetInitializer<OrderDbContext>(null);
        }
    }
}
