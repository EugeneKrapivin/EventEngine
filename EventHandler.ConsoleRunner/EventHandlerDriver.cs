using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using EventHandlerLogic;
using StackExchange.Redis;
using StackExchange.Redis.Extensions.Core;
using StackExchange.Redis.Extensions.Newtonsoft;
using StatsdClient;

namespace EventHandler.ConsoleRunner
{
    public class EventHandlerDriver
    {
        private readonly StackExchangeRedisCacheClient _cacheClient;
        private static CancellationTokenSource _cancellationTokenSource;

        public EventHandlerDriver()
        {
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
            new EventHandlerDriver()
                .RunAsync(cancellationToken)
                .Wait(cancellationToken);
        }

        public async Task RunAsync(CancellationToken cancellationToken)
        {
            await new HandlerExecutor(_cacheClient, $"{Process.GetCurrentProcess().ProcessName}-{Process.GetCurrentProcess().Id}")
                .HandleAsync(cancellationToken);
        }
    }
}
