using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using EventDispatcherLogic;
using StackExchange.Redis.Extensions.Core;
using StackExchange.Redis.Extensions.Newtonsoft;
using StatsdClient;

namespace EventDispatcher.ConsoleRunner
{
    class EventDispatcherDriver
    {
        private readonly StackExchangeRedisCacheClient _cacheClient;
        private static CancellationTokenSource _cancellationTokenSource;
        public EventDispatcherDriver()
        {
            _cancellationTokenSource = new CancellationTokenSource();
            AppDomain.CurrentDomain.ProcessExit += OnProcessExit;

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
            var mux = ConnectionMultiplexer.Connect(new ConfigurationOptions
            {
                EndPoints = { new DnsEndPoint("docker.local", 6379) }
            });
            Console.WriteLine("Done.");
            _cacheClient = new StackExchangeRedisCacheClient(mux, new NewtonsoftSerializer());
        }

        private void OnProcessExit(object sender, EventArgs e)
        {
            _cancellationTokenSource.Cancel();
        }

        static void Main(string[] args)
        {
            _cancellationTokenSource = new CancellationTokenSource();
            var cancellationToken = _cancellationTokenSource.Token;
            new EventDispatcherDriver().RunAsync(cancellationToken).Wait(cancellationToken);
        }

        public async Task RunAsync(CancellationToken cancellationToken)
        {
            await new DispatchExecutor(_cacheClient)
                .DispatchAsync(cancellationToken);
        }
    }
}
