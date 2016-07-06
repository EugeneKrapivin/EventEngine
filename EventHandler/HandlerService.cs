using System;
using System.Linq;
using System.Net;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Models;
using Newtonsoft.Json;
using StackExchange.Redis;
using StackExchange.Redis.Extensions.Core;
using StackExchange.Redis.Extensions.Core.Extensions;
using StackExchange.Redis.Extensions.Newtonsoft;
using StatsdClient;

namespace EventHandler
{
    class HandlerService
    {
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
                    Metrics.GaugeDelta("system.active-handlers", -1);
                    break;
            }
            return true;
        }
        public static void Main(string[] args)
        {
            Console.WriteLine("Starting handler...");
            Console.Write("Connecting to redis...");
            var mux = ConnectionMultiplexer.Connect(new ConfigurationOptions
            {
                EndPoints = { new DnsEndPoint("docker.local", 6379) }
            });
            Console.WriteLine("Done.");
            Console.Write("Connecting to monitor...");
            Metrics.Configure(new MetricsConfig
            {
                StatsdServerName = "docker.local",
                StatsdServerPort = 32768,
                UseTcpProtocol = false,
                Prefix = "event-engine-poc"
            });
            Console.WriteLine("Done.");
            Console.WriteLine("Handler started.");
            Metrics.GaugeDelta("system.active-handlers", +1);

            SetConsoleCtrlHandler(new HandlerRoutine(ConsoleCtrlCheck), true);

            var cacheClient = new StackExchangeRedisCacheClient(mux, new NewtonsoftSerializer());

            do
            {
                Parallel.ForEach(cacheClient.Database.SetScan("WorkSet"), 
                    new ParallelOptions { MaxDegreeOfParallelism = 4 },
                    tenantSetKey => HandleMessage(cacheClient, int.Parse(tenantSetKey)));
            } while (cacheClient.Database.SetLength("WorkSet") > 0);


            cacheClient.Subscribe<int>("WorkWorkWork", tenantSetKey => HandleMessage(cacheClient, tenantSetKey));

            Console.WriteLine("No data in queue");
            Console.ReadKey();
        }

        private static void HandleMessage(StackExchangeRedisCacheClient cacheClient, int tenantSetKey)
        {

            var instanceQueueKey = cacheClient.Database.SetPop($"TenantWorkSets:{tenantSetKey}");
            var handlerPrefix = $"handler.{System.Diagnostics.Process.GetCurrentProcess().Id}";
            if (!instanceQueueKey.HasValue)
            {
                return;
            }

            Metrics.GaugeDelta("instances.currently-in-process",1);
            do
            {
                var cacheData = cacheClient.Database.ListRightPop($"InstanceLists:{instanceQueueKey}");

                if (!cacheData.HasValue)
                {
                    break;
                }

                var data = cacheClient.Serializer.Deserialize<DummyInstanceMessage>(cacheData);

                if (data == null)
                {
                    Metrics.Counter($"{handlerPrefix}.failed-deserialize");
                    break;
                }

                using (Metrics.StartTimer($"{handlerPrefix}.processing-time"))
                {
                    Process(data);
                }
            } while (true);
            Metrics.GaugeDelta("instances.currently-in-process", -1);
            Metrics.GaugeDelta("instances", -1);
        }

        private static void Process(DummyInstanceMessage data)
        {
            var r = new Random((int)DateTime.UtcNow.Ticks);
            Console.WriteLine($"{Thread.CurrentThread.ManagedThreadId} - {data.OriginalMessage.TenantId}:{data.OriginalMessage.InstanceId} -> {data.Data}");
            Thread.Sleep(r.Next(20, 150));
        }
    }
}
