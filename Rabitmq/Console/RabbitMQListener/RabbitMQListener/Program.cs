using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

const string MainQueue = "locationSampleQueue";
const string DlxExchange = "location.dlx";
const string DlxQueue = "location.dlq";
const string DlxRoutingKey = "location.dead";

Console.OutputEncoding = Encoding.UTF8;
Console.WriteLine("==================================================");
Console.WriteLine("  RabbitMQ Consumer with Dead Letter Queue (DLQ)  ");
Console.WriteLine("==================================================\n");

var factory = new ConnectionFactory { HostName = "localhost" };

await using var connection = await factory.CreateConnectionAsync();
await using var mainChannel = await connection.CreateChannelAsync();
await using var dlqChannel = await connection.CreateChannelAsync();

// 1. Declare the Dead Letter Exchange (DLX) & Dead Letter Queue (DLQ)
await mainChannel.ExchangeDeclareAsync(
    exchange: DlxExchange,
    type: ExchangeType.Direct,
    durable: true,
    autoDelete: false);

await mainChannel.QueueDeclareAsync(
    queue: DlxQueue,
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: null);

await mainChannel.QueueBindAsync(
    queue: DlxQueue,
    exchange: DlxExchange,
    routingKey: DlxRoutingKey);

// 2. Declare Main Queue linked to Dead Letter Exchange
var mainQueueArgs = new Dictionary<string, object?>
{
    { "x-dead-letter-exchange", DlxExchange },
    { "x-dead-letter-routing-key", DlxRoutingKey }
};

await mainChannel.QueueDeclareAsync(
    queue: MainQueue,
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: mainQueueArgs);

await mainChannel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);
await dlqChannel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

Console.WriteLine($"[Setup] DLX: '{DlxExchange}' -> DLQ: '{DlxQueue}' (RoutingKey: '{DlxRoutingKey}')");
Console.WriteLine($"[Setup] Main Queue: '{MainQueue}' linked to DLX.");
Console.WriteLine("\n[Status] Listening to queues. Waiting for messages...\n");

// ==========================================
// 3. MAIN QUEUE CONSUMER
// ==========================================
var mainConsumer = new AsyncEventingBasicConsumer(mainChannel);
mainConsumer.ReceivedAsync += async (sender, ea) =>
{
    var body = ea.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);

    Console.ForegroundColor = ConsoleColor.Cyan;
    Console.WriteLine($"\n[Main Queue] Received message: \"{message}\"");
    Console.ResetColor();

    try
    {
        // Business logic & validation:
        // Latitude must be between -90 and 90, Longitude between -180 and 180.
        // If message contains invalid coordinates or "FAIL", we reject it.
        bool isInvalid = message.Contains("FAIL", StringComparison.OrdinalIgnoreCase);

        if (!isInvalid && TryExtractCoordinates(message, out double lat, out double lon))
        {
            if (lat < -90 || lat > 90 || lon < -180 || lon > 180)
            {
                isInvalid = true;
            }
        }

        if (isInvalid)
        {
            throw new InvalidOperationException($"Invalid coordinates or simulated processing failure in message: '{message}'");
        }

        // Processing simulated work
        await Task.Delay(500);

        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine($"[Main Queue] \u2714 Processed successfully: \"{message}\"");
        Console.ResetColor();

        // Success: Acknowledge message
        await mainChannel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
    }
    catch (Exception ex)
    {
        Console.ForegroundColor = ConsoleColor.Red;
        Console.WriteLine($"[Main Queue] \u2716 Error: {ex.Message}");
        Console.WriteLine($"[Main Queue] -> Rejecting message with requeue=false (RabbitMQ will route to DLQ)...");
        Console.ResetColor();

        // Negative Acknowledgment with requeue: false
        // RabbitMQ automatically routes this message to the Dead Letter Exchange
        await mainChannel.BasicNackAsync(
            deliveryTag: ea.DeliveryTag,
            multiple: false,
            requeue: false);
    }
};

await mainChannel.BasicConsumeAsync(
    queue: MainQueue,
    autoAck: false,
    consumer: mainConsumer);

// ==========================================
// 4. DEAD LETTER QUEUE (DLQ) CONSUMER
// ==========================================
var dlqConsumer = new AsyncEventingBasicConsumer(dlqChannel);
dlqConsumer.ReceivedAsync += async (sender, ea) =>
{
    var body = ea.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);

    string deathReason = "unknown";
    string originalQueue = MainQueue;

    // Inspect x-death header populated by RabbitMQ
    if (ea.BasicProperties.Headers != null && ea.BasicProperties.Headers.TryGetValue("x-death", out var xDeathObj))
    {
        if (xDeathObj is List<object> deathList && deathList.Count > 0 && deathList[0] is Dictionary<string, object?> deathMap)
        {
            if (deathMap.TryGetValue("reason", out var reasonBytes) && reasonBytes is byte[] rBytes)
                deathReason = Encoding.UTF8.GetString(rBytes);
            else if (deathMap.TryGetValue("reason", out var reasonStr))
                deathReason = reasonStr?.ToString() ?? "unknown";

            if (deathMap.TryGetValue("queue", out var queueBytes) && queueBytes is byte[] qBytes)
                originalQueue = Encoding.UTF8.GetString(qBytes);
            else if (deathMap.TryGetValue("queue", out var queueStr))
                originalQueue = queueStr?.ToString() ?? MainQueue;
        }
    }

    Console.ForegroundColor = ConsoleColor.Yellow;
    Console.WriteLine($"\n[DLQ Consumer] \u26a0 Received Dead-Lettered Message!");
    Console.WriteLine($"   Payload:        \"{message}\"");
    Console.WriteLine($"   Death Reason:   {deathReason}");
    Console.WriteLine($"   Original Queue: {originalQueue}");
    Console.ResetColor();

    // Acknowledge receipt from DLQ
    await dlqChannel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
};

await dlqChannel.BasicConsumeAsync(
    queue: DlxQueue,
    autoAck: false,
    consumer: dlqConsumer);

Console.WriteLine("Press [Enter] to exit.\n");
Console.ReadLine();

// Helper method to parse latitude and longitude from string format
static bool TryExtractCoordinates(string message, out double latitude, out double longitude)
{
    latitude = 0;
    longitude = 0;
    try
    {
        // Example: "Latitude: 235235, Longitude: 5353 and Time: 09-10-2026 09:34:08"
        var parts = message.Split(new[] { ',', ' ' }, StringSplitOptions.RemoveEmptyEntries);
        for (int i = 0; i < parts.Length - 1; i++)
        {
            if (parts[i].Equals("Latitude:", StringComparison.OrdinalIgnoreCase) && double.TryParse(parts[i + 1].Trim(','), out var lat))
            {
                latitude = lat;
            }
            if (parts[i].Equals("Longitude:", StringComparison.OrdinalIgnoreCase) && double.TryParse(parts[i + 1].Trim(','), out var lon))
            {
                longitude = lon;
            }
        }
        return true;
    }
    catch
    {
        return false;
    }
}