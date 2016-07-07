using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using EventDispatcherLogic;
using EventModels;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Diagnostics;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage;
using StackExchange.Redis;
using StackExchange.Redis.Extensions.Core;
using StackExchange.Redis.Extensions.Newtonsoft;
using StatsdClient;

namespace EventDispatcher
{
    public class EventDispatcherService : RoleEntryPoint
    {
        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        private readonly ManualResetEvent _runCompleteEvent = new ManualResetEvent(false);
        private ConnectionMultiplexer _mux;
        private StackExchangeRedisCacheClient _cacheClient;

        public override void Run()
        {
            Trace.TraceInformation("Event Dispatcher is running");

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

            Metrics.Configure(new MetricsConfig
            {
                StatsdServerName = "docker.local",
                StatsdServerPort = 32768,
                UseTcpProtocol = false,
                Prefix = "event-engine-poc"
            });

            Metrics.GaugeDelta("system.active-dispatchers", +1);

            _mux = ConnectionMultiplexer.Connect(new ConfigurationOptions
            {
                EndPoints = { new DnsEndPoint("docker.local", 6379) },
                AllowAdmin = true
            });

            _cacheClient = new StackExchangeRedisCacheClient(_mux, new NewtonsoftSerializer());

            Trace.TraceInformation("Event Dispatcher has been started");

            return result;
        }

        public override void OnStop()
        {
            Trace.TraceInformation("Event Dispatcher is stopping");

            this._cancellationTokenSource.Cancel();
            this._runCompleteEvent.WaitOne();

            base.OnStop();
            Metrics.GaugeDelta("system.active-dispatchers", -1);

            Trace.TraceInformation("Event Dispatcher has stopped");
        }

        private async Task RunAsync(CancellationToken cancellationToken)
        {
            await new DispatchExecutor(_cacheClient)
                .DispatchAsync(cancellationToken);
        }
    }
}
