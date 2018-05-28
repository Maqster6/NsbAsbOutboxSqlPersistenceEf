using System;
using System.Data.Entity;
using NServiceBus;

namespace Infra.NServiceBus.Persistence
{
    /// <summary>
    /// This only exist to warp the extension method as these cannot easily to mocked for unit testing.
    /// </summary>
    class DbContextWrapper<TDbContext> : IDbContextWrapper<TDbContext> where TDbContext : DbContext
    {
        public TDbContext GetDbContext(IMessageHandlerContext context)
        {
            if (!context.Extensions.TryGet(out TDbContext dataContext)) throw new Exception($"No dbcontext set for '{typeof(TDbContext)}.");
            return dataContext;
        }
    }
}