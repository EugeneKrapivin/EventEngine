using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Models;
using StackExchange.Redis;
using StackExchange.Redis.Extensions.Core;
using StackExchange.Redis.Extensions.Newtonsoft;
using StatsdClient;

namespace EventDispatcher
{
    class DispatcherService
    {
        const int TenantsCount = 4;
        const int InstnacesCount = 3000;

        [DllImport("Kernel32")]
        public static extern bool SetConsoleCtrlHandler(HandlerRoutine Handler, bool Add);

        // A delegate type to be used as the handler routine 
        // for SetConsoleCtrlHandler.
        public delegate bool HandlerRoutine(CtrlTypes CtrlType);

        // An enumerated type for the control messages
        // sent to the handler routine.
        public enum CtrlTypes
        {
            CTRL_C_EVENT = 0,
            CTRL_BREAK_EVENT,
            CTRL_CLOSE_EVENT,
            CTRL_LOGOFF_EVENT = 5,
            CTRL_SHUTDOWN_EVENT
        }

        private static bool ConsoleCtrlCheck(CtrlTypes ctrlType)
        {
            switch (ctrlType)
            {
                case CtrlTypes.CTRL_BREAK_EVENT:
                case CtrlTypes.CTRL_CLOSE_EVENT:
                case CtrlTypes.CTRL_C_EVENT:
                case CtrlTypes.CTRL_SHUTDOWN_EVENT:
                    Metrics.GaugeDelta("system.active-dispatchers", -1);
                    break;
            }
            return true;
        }

        static void Main()
        {
            var mux = ConnectionMultiplexer.Connect(new ConfigurationOptions
            {
                EndPoints = { new DnsEndPoint("docker.local", 6379) },
                AllowAdmin = true
            });
            Metrics.Configure(new MetricsConfig
            {
                StatsdServerName = "docker.local",
                StatsdServerPort = 32768,
                UseTcpProtocol = false,
                Prefix = "event-engine-poc"
            });
            var cacheClient = new StackExchangeRedisCacheClient(mux, new NewtonsoftSerializer());
            Metrics.GaugeAbsoluteValue("instances", 0); // clean slate each time
            cacheClient.FlushDb(); // clean slate each time
            //Metrics.GaugeAbsoluteValue("system.active-handlers", 0);
            Metrics.GaugeAbsoluteValue("system.active-dispatchers", 0);
            Metrics.GaugeDelta("system.active-dispatchers", +1);

            SetConsoleCtrlHandler(new HandlerRoutine(ConsoleCtrlCheck), true);

            foreach (var msg in GetDataStream()) // infinite loop
            {
                var instanceQueueKey = $"{msg.InstanceId}";
                var tenantSetKey = $"{msg.TenantId}";

                var data = new DummyInstanceMessage
                {
                    Data = Guid.NewGuid(),
                    OriginalMessage = msg
                };


                Metrics.GaugeDelta("instances.instance-list-write-locks", 1);
                cacheClient.Database.ListLeftPush($"InstanceLists:{instanceQueueKey}",cacheClient.Serializer.Serialize(data));
                Metrics.Counter("instances.events");

                cacheClient.Database.LockRelease("InstanceListWriteLocks", instanceQueueKey);
                Metrics.GaugeDelta("instances.instance-list-write-locks", -1);


                if (cacheClient.Database.SetAdd($"TenantWorkSets:{tenantSetKey}", instanceQueueKey))
                {
                    Metrics.GaugeDelta("instances", 1);
                }
                cacheClient.Database.SetAdd("WorkSet", tenantSetKey);

                Console.WriteLine($"Added to: {tenantSetKey}:{instanceQueueKey}");

                cacheClient.Database.Publish("WorkWorkWork", tenantSetKey);

                Thread.Sleep(10);
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
