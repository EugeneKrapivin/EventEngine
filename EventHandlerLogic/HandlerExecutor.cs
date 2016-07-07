using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using EventModels;
using StackExchange.Redis;
using StackExchange.Redis.Extensions.Core;
using StatsdClient;

namespace EventHandlerLogic
{
    public class HandlerExecutor
    {
        private readonly StackExchangeRedisCacheClient _cacheClient;
        private readonly string _executorName;

        public HandlerExecutor(StackExchangeRedisCacheClient cacheClient, string executorName)
        {
            if (executorName == null) throw new ArgumentNullException(nameof(executorName));
            _cacheClient = cacheClient;
            _executorName = executorName;
        }

        public async Task HandleAsync(CancellationToken cancellationToken)
        {
            Parallel.ForEach(_cacheClient.Database.SetScan("WorkSet"),
                new ParallelOptions {MaxDegreeOfParallelism = 4},
                async tenantSetKey =>
                    await HandleSingleTenantInstanceAsync(int.Parse(tenantSetKey), cancellationToken));
            
            await _cacheClient.SubscribeAsync<int>("WorkWorkWork", x => HandleSingleTenantInstanceAsync(x, cancellationToken));

            await Task.Delay(TimeSpan.MaxValue, cancellationToken);
        }

        private async Task HandleSingleTenantInstanceAsync(int tenantSetKey, CancellationToken cancellationToken)
        {
            var instanceQueueKey = await _cacheClient.Database.SetPopAsync($"TenantWorkSets:{tenantSetKey}");

            if (!instanceQueueKey.HasValue)
            {
                try
                {
#pragma warning disable 4014
                    var trans = _cacheClient.Database.CreateTransaction();
                    trans.AddCondition(Condition.SetLengthEqual($"TenantWorkSets:{tenantSetKey}", 0));
                    trans.SetRemoveAsync("WorkSet", tenantSetKey);
#pragma warning restore 4014
                    if (await _cacheClient.Database.LockTakeAsync("GlobalWorkSet", tenantSetKey, TimeSpan.FromMilliseconds(2000)))
                    {
                        await trans.ExecuteAsync();
                    }
                }
                finally
                {
                    await _cacheClient.Database.LockReleaseAsync("GlobalWorkSet", tenantSetKey);
                }

                return;

            }

            await HandleInstanceMessagesAsync(Guid.Parse(instanceQueueKey), cancellationToken);
            Metrics.Counter("instances", -1);
        }

        private async Task HandleInstanceMessagesAsync(Guid instanceQueueKey, CancellationToken cancellationToken)
        {
            var handlerPrefix = $"handler.{_executorName}";

            Metrics.Counter("instances.currently-in-process");

            do
            {
                var cacheData = await _cacheClient.Database.ListRightPopAsync($"InstanceLists:{instanceQueueKey}");

                if (!cacheData.HasValue)
                {
                    break;
                }

                var data = _cacheClient.Serializer.Deserialize<DummyInstanceMessage>(cacheData);

                if (data == null)
                {
                    Metrics.Counter($"{handlerPrefix}.failed-deserialize");
                    break;
                }

                using (Metrics.StartTimer($"{handlerPrefix}.processing-time"))
                {
                    await ProcessAsync(data, cancellationToken);
                }
            } while (true && !cancellationToken.IsCancellationRequested);

            Metrics.Counter("instances.currently-in-process", -1);
        }

        private static async Task ProcessAsync(DummyInstanceMessage data, CancellationToken cancellationToken)
        {
            var r = new Random((int)DateTime.UtcNow.Ticks);
            Console.WriteLine($"{Thread.CurrentThread.ManagedThreadId} - {data.OriginalMessage.TenantId}:{data.OriginalMessage.InstanceId} -> {data.Data}");
            await Task.Delay(TimeSpan.FromMilliseconds(r.Next(20, 750)), cancellationToken);
        }
    }
}
