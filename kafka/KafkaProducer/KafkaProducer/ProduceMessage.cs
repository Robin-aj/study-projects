using Confluent.Kafka;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaProducer
{
    public class ProduceMessage
    {
        public async Task CreateMessage()
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                ClientId = "my-app",
                BrokerAddressFamily = BrokerAddressFamily.V4,
            };

            using var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            using var producer = new ProducerBuilder<Null, string>(config).Build();

            Console.WriteLine("Select message sending mode:");
            Console.WriteLine("  1. Manual loop (type messages continuously, 'exit' to quit)");
            Console.WriteLine("  2. Automated stream (sends a message every 1 second)");
            Console.Write("Enter choice [1 or 2, default is 1]: ");
            var choice = Console.ReadLine()?.Trim();

            if (choice == "2")
            {
                Console.WriteLine("\nStreaming messages automatically. Press Ctrl+C to stop...\n");
                int counter = 1;
                try
                {
                    while (!cts.Token.IsCancellationRequested)
                    {
                        var text = $"Message #{counter++} at {DateTime.Now:HH:mm:ss}";
                        var message = new Message<Null, string> { Value = text };

                        var deliveryReport = await producer.ProduceAsync("my-topic", message, cts.Token);
                        Console.WriteLine($"Delivered: '{text}' -> {deliveryReport.TopicPartitionOffset}");

                        await Task.Delay(1000, cts.Token);
                    }
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("\nStopping automated stream...");
                }
            }
            else
            {
                Console.WriteLine("\nEnter messages to send (type 'exit' or press Ctrl+C to stop):");
                try
                {
                    while (!cts.Token.IsCancellationRequested)
                    {
                        Console.Write("> ");
                        var input = Console.ReadLine();

                        if (input == null || input.Equals("exit", StringComparison.OrdinalIgnoreCase))
                        {
                            break;
                        }

                        if (string.IsNullOrWhiteSpace(input))
                        {
                            continue;
                        }

                        var message = new Message<Null, string> { Value = input };
                        var deliveryReport = await producer.ProduceAsync("my-topic", message, cts.Token);
                        Console.WriteLine($"Delivered to {deliveryReport.TopicPartitionOffset}");
                    }
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("\nStopping manual producer...");
                }
            }

            Console.WriteLine("Flushing buffered messages...");
            producer.Flush(TimeSpan.FromSeconds(5));
            Console.WriteLine("Producer finished.");
        }
    }
}
