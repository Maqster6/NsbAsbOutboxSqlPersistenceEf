using NServiceBus;

namespace Infra.NServiceBus.Persistence
{
    public interface IDbContextWrapper<out TDbContext>
    {
        TDbContext GetDbContext(IMessageHandlerContext session);
    }
}