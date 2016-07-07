using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using EventHandlerLogic;
using EventModels;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Diagnostics;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage;
using StackExchange.Redis;
using StackExchange.Redis.Extensions.Core;
using StackExchange.Redis.Extensions.Newtonsoft;
using StatsdClient;

namespace EventHandler
{
    public class EventHandlerService : RoleEntryPoint
    {
        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        private readonly ManualResetEvent _runCompleteEvent = new ManualResetEvent(false);
        private ConnectionMultiplexer _mux;
        private StackExchangeRedisCacheClient _cacheClient;

        public override void Run()
        {
            Trace.TraceInformation("Event Handler is running");

            try
            {
                RunAsync(_cancellationTokenSource.Token).Wait(_cancellationTokenSource.Token);
            }
            catch (Exception)
            {

            }
            finally
            {
                _runCompleteEvent.Set();
            }
        }

        public override bool OnStart()
        {
            ServicePointManager.DefaultConnectionLimit = 12;


            var result = base.OnStart();
            Console.WriteLine("Starting Event Handler...");
            Console.Write("Connecting to monitor...");
            Metrics.Configure(new MetricsConfig
            {
                StatsdServerName = "docker.local",
                StatsdServerPort = 32768,
                UseTcpProtocol = false,
                Prefix = "event-engine-poc"
            });
            Console.WriteLine("Done.");

            Metrics.GaugeDelta("system.active-handlers", +1);
            Console.Write("Connecting to Redis...");
            _mux = ConnectionMultiplexer.Connect(new ConfigurationOptions
            {
                EndPoints = { new DnsEndPoint("docker.local", 6379) }
            });
            Console.WriteLine("Done.");
            _cacheClient = new StackExchangeRedisCacheClient(_mux, new NewtonsoftSerializer());

            Trace.TraceInformation("EventHandler has been started");

            return result;
        }

        public override void OnStop()
        {
            Trace.TraceInformation("Event Handler is stopping");

            this._cancellationTokenSource.Cancel();
            this._runCompleteEvent.WaitOne();

            base.OnStop();
            Metrics.GaugeDelta("system.active-handlers", -1);

            Trace.TraceInformation("Event Handler has stopped");
        }

        private async Task RunAsync(CancellationToken cancellationToken)
        {

            await new HandlerExecutor(_cacheClient, RoleEnvironment.CurrentRoleInstance.Id.Replace('.', '-'))
                .HandleAsync(cancellationToken);
        }
    }
}
