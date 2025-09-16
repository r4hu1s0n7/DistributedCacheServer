using DistributedCacheServer;
using Microsoft.Extensions.Configuration;
using System.Net;
using System.Net.Sockets;
using System.Text;
var config = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();

const int ReadBufferSize = 4096 * 2;
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
            List<object> Response = new List<object>();
            var buffer = new byte[ReadBufferSize];
            int bytesRead = await stream.ReadAsync(buffer, 0, ReadBufferSize);
            if (bytesRead == 0) // Client disconnected
            {
                Console.WriteLine($"Client disconnected: {client.Client.RemoteEndPoint}");
                break;
            }
            var commands = RESP.ParseBatchCommands(buffer.Take(bytesRead).ToArray());
            foreach (var command in commands)
            {
                object commandResponse;
                try
                {
                    var param = Command.Parse(command.ToArray());
                    commandResponse = Command.Execute(param);
                }
                catch (CacheException ce)
                {
                    commandResponse = ce.Message;
                     
                }
                Response.Add(commandResponse);

            }
            var responseBuffer = RESP.SerializeBatchResponse(Response.ToArray());
            await stream.WriteAsync(responseBuffer, 0, responseBuffer.Length);
        }
    }
}