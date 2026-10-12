using Grpc.Net.Client;
using GrpcService;

// Connect to the gRPC service running on port 5273 (HTTP)
using var channel = GrpcChannel.ForAddress("http://localhost:5273");
var client = new Greeter.GreeterClient(channel);

Console.WriteLine("=== Connected to gRPC Server ===");

while (true)
{
    Console.Write("\nEnter a name (or type 'exit' to quit): ");
    var input = Console.ReadLine();

    if (string.IsNullOrWhiteSpace(input) || input.Trim().Equals("exit", StringComparison.OrdinalIgnoreCase))
    {
        Console.WriteLine("Exiting client...");
        break;
    }

    try
    {
        var serverReply = await client.SayHelloAsync(new HelloRequest { Name = input.Trim() });
        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine($"Response from Server: {serverReply.Message}");
        Console.ResetColor();
    }
    catch (Exception ex)
    {
        Console.ForegroundColor = ConsoleColor.Red;
        Console.WriteLine($"Error calling server: {ex.Message}");
        Console.ResetColor();
    }
}
