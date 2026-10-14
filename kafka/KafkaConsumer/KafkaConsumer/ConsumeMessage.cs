using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace KafkaConsumer
{
    public class ConsumeMessage
    {
        public void ReadMessage()
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                ClientId = "my-app",
                GroupId = "my-group",
                BrokerAddressFamily = BrokerAddressFamily.V4,
            };

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true; // prevent the process from terminating immediately
                cts.Cancel();
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
            consumer.Subscribe("my-topic");

            Console.WriteLine("Consumer started. Listening for messages on 'my-topic'... (Press Ctrl+C to exit)");

            try
            {
                while (true)
                {
                    var consumeResult = consumer.Consume(cts.Token);
                    Console.WriteLine($"Message received from {consumeResult.TopicPartitionOffset}: {consumeResult.Message.Value}");
                }
            }
            catch (OperationCanceledException)
            {
                // The consumer was stopped via cancellation token.
                Console.WriteLine("Consumer shutting down gracefully...");
            }
            finally
            {
                consumer.Close();
            }
        }
    }
}
