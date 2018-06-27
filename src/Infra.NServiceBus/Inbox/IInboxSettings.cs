using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Infra.NServiceBus.Inbox
{
    public interface IInboxSettings
    {
        string SchemaName { get; set; }
    }
}
