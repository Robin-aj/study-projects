using Microsoft.AspNetCore.Mvc;
using RabbitMQ.Client;
using System.Text;

namespace RabbitmqAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class LocationController : ControllerBase
    {
        private const string MainQueue = "locationSampleQueue";
        private const string DlxExchange = "location.dlx";
        private const string DlxQueue = "location.dlq";
        private const string DlxRoutingKey = "location.dead";

        [HttpPost]
        public async Task<IActionResult> Post([FromBody] Location location)
        {
            var factory = new ConnectionFactory { HostName = "localhost" };

            await using var connection = await factory.CreateConnectionAsync();
            await using var channel = await connection.CreateChannelAsync();

            // 1. Declare the Dead Letter Exchange (DLX) & Dead Letter Queue (DLQ)
            await channel.ExchangeDeclareAsync(
                exchange: DlxExchange,
                type: ExchangeType.Direct,
                durable: true,
                autoDelete: false);

            await channel.QueueDeclareAsync(
                queue: DlxQueue,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null);

            await channel.QueueBindAsync(
                queue: DlxQueue,
                exchange: DlxExchange,
                routingKey: DlxRoutingKey);

            // 2. Declare the Main Queue with Dead Letter Queue configuration
            var mainQueueArgs = new Dictionary<string, object?>
            {
                { "x-dead-letter-exchange", DlxExchange },
                { "x-dead-letter-routing-key", DlxRoutingKey }
            };

            await channel.QueueDeclareAsync(
                queue: MainQueue,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: mainQueueArgs);

            // 3. Publish message to Main Queue
            string message = $"Latitude: {location.Latitude}, Longitude: {location.Longitude} and Time: {location.Date}";
            var body = Encoding.UTF8.GetBytes(message);

            await channel.BasicPublishAsync(
                exchange: string.Empty,
                routingKey: MainQueue,
                body: body);

            return Ok(new
            {
                status = "Message published to RabbitMQ",
                message = message,
                queue = MainQueue
            });
        }
    }
}
