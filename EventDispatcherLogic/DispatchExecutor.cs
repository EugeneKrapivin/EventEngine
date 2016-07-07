using EventModels;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis.Extensions.Core;
using StatsdClient;

namespace EventDispatcherLogic
{
    public class DispatchExecutor
    {
        private readonly StackExchangeRedisCacheClient _cacheClient;
        private const int TenantsCount = 70;
        private const int InstnacesCount = 30000;

        public DispatchExecutor(StackExchangeRedisCacheClient cacheClient)
        {
            if (cacheClient == null) throw new ArgumentNullException(nameof(cacheClient));
            _cacheClient = cacheClient;
        }

        public async Task DispatchAsync(CancellationToken cancellationToken)
        {
            var r = new Random((int)DateTime.UtcNow.Ticks);
            while (!cancellationToken.IsCancellationRequested)
            {
                foreach (var msg in GetDataStream())
                {
                    if (cancellationToken.IsCancellationRequested) break;

                    var instanceQueueKey = $"{msg.InstanceId}";
                    var tenantSetKey = $"{msg.TenantId}";

                    var data = new DummyInstanceMessage
                    {
                        Data = Guid.NewGuid(),
                        OriginalMessage = msg
                    };

                    var trans = _cacheClient.Database.CreateTransaction();

#pragma warning disable 4014
                    trans.ListLeftPushAsync($"InstanceLists:{instanceQueueKey}", _cacheClient.Serializer.Serialize(data));
                    var wasAdded = trans.SetAddAsync($"TenantWorkSets:{tenantSetKey}", instanceQueueKey);
                    trans.SetAddAsync("WorkSet", tenantSetKey);
                    trans.PublishAsync("WorkWorkWork", tenantSetKey);
#pragma warning restore 4014

                    try
                    {
                        if (await _cacheClient.Database.LockTakeAsync("GlobalWorkSet", tenantSetKey, TimeSpan.FromMilliseconds(2000)))
                        {
                            if (await trans.ExecuteAsync())
                            {
                                if (wasAdded.Result)
                                {
                                    Metrics.Counter("instances");
                                }
                                Metrics.Counter("instances.events");
                                Console.WriteLine($"Added to: {tenantSetKey}:{instanceQueueKey}");
                            }
                        }
                    }
                    finally
                    {
                        await _cacheClient.Database.LockReleaseAsync("GlobalWorkSet", tenantSetKey);
                    }
                    
                    await Task.Delay(r.Next(0, 25), cancellationToken);
                }
            }
        }

        public static IEnumerable<DummyQueueMessage> GetDataStream()
        {
            var tenants = Enumerable.Range(0, TenantsCount).Select(x => x).ToArray();
            var instances = tenants.ToDictionary(key => key, value => Enumerable.Range(0, InstnacesCount).Select(x => Guid.NewGuid()).ToArray());
            var random = new Random((int)(DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0)).TotalSeconds);

            while (true)
            {
                var tid = random.Next(TenantsCount);
                yield return new DummyQueueMessage
                {
                    TenantId = tenants[tid],
                    InstanceId = instances[tenants[tid]][random.Next(InstnacesCount)]
                };
            }
        }
    }
}
