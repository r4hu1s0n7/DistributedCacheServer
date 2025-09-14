using DistributedCacheServer;
using Microsoft.Extensions.Configuration;
using System.Net;
using System.Net.Sockets;
using System.Text;
var config = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();

const int ReadBufferSize = 1024 * 2;
int Port = Convert.ToInt32(config["ClientPort"]);
string IP = config["ClientIP"];
IPAddress IPaddr = IPAddress.Parse(IP);
var listener = new TcpListener(IPaddr, Port);
            
Console.WriteLine("CACHE Server:");

Persistance.Instance.LoadStorage();

Persistance.Instance.StartPersistance();



listener.Start(); // configurable connections backlog
Console.WriteLine("listening");


while (true)
{
    var client = await listener.AcceptTcpClientAsync();
    _ = Task.Run(() => HandleClientAsync(client)); // Offload to Task
}

async Task HandleClientAsync(TcpClient client)
{
    using (client)
    using (var stream = client.GetStream())
    {
        client.ReceiveTimeout = 5000; // 5 seconds
        
        while (client.Connected)
        {
            var buffer = new byte[ReadBufferSize];
            int bytesRead = await stream.ReadAsync(buffer, 0, ReadBufferSize);
            if (bytesRead == 0) // Client disconnected
            {
                Console.WriteLine($"Client disconnected: {client.Client.RemoteEndPoint}");
                break;
            }
            var commandArgs = RESP.Deserialize(buffer.Take(bytesRead).ToArray());
            object response;
            try
            {
                var command = Command.Parse(commandArgs.ToArray());
                response = Command.Execute(command);
            }
            catch (CacheException ce)
            {
                response = ce.Message;
            }
            var responseBuffer = RESP.Serialize(response);
            await stream.WriteAsync(responseBuffer, 0, responseBuffer.Length);
        }
    }
}