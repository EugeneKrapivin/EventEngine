using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventModels
{
    public class DummyInstanceMessage
    {
        public DummyQueueMessage OriginalMessage { get; set; }
        public Guid Data { get; set; }
    }

    public class DummyQueueMessage
    {
        public int TenantId { get; set; }
        public Guid InstanceId { get; set; }
    }
}
