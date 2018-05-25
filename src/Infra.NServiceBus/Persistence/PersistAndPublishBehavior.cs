using System;
using System.Data.Entity;
using System.Threading.Tasks;
using NServiceBus.Pipeline;

namespace Infra.NServiceBus.Persistence
{
    public class PersistAndPublishBehavior<TDbContext> : Behavior<IInvokeHandlerContext> where TDbContext : DbContext
    {
      
        public override async Task Invoke(IInvokeHandlerContext context, Func<Task> next)
        {
            var dataContext = context.SynchronizedStorageSession.FromCurrentSession<TDbContext>();
            using (dataContext)
            {
                context.Extensions.Set("DbContext", dataContext);

                await next().ConfigureAwait(false);

                await DataContextPersistence.SaveChangesAsync(dataContext);
                
                //Here I also want to find all effected dbcontext modified entities and in them there aggregateroots
                //and then publish the uncommitedevents that they have. I have removed this from this sample cause I do not 
                //think it is important to why it is not able to save without throwing exceptions and loosing data.
            }


        }
    }
}
