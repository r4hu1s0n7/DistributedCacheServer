using DistributedCacheServer;
using System.Net;
using System.Net.Sockets;
using System.Text;


const int ReadBufferSize = 1024 * 2;
const int Port = 9500;
IPAddress IPaddr = IPAddress.Parse("127.0.0.1");
var listener = new TcpListener(IPaddr, Port);

Console.WriteLine("CACHE Server:");

listener.Start();
Console.WriteLine("listening");


while (true)
{
    using (var client = listener.AcceptTcpClient())
    using (var stream = client.GetStream())
    {
        Console.WriteLine("Client connected.");
        var buffer = new byte[ReadBufferSize];

        int bytesReadSize = stream.Read(buffer, 0, ReadBufferSize);
        Console.WriteLine("Command Received");

        var commandArgs = RESP.Deserialize(buffer.Take(bytesReadSize).ToArray());


        object response = null;
        try
        {
            Command executableCommand = Command.Parse(commandArgs.ToArray());

            response = PerformExecution(executableCommand);

        }catch (CacheException ce)
        {
            response = ce.Message;   
        }

        buffer = RESP.Serialize(response);
        stream.Write(buffer);
    }
}

object PerformExecution(Command executableCommand)
{
    switch (executableCommand.Type)
    {
        case CommandType.STORAGE:
            if (executableCommand.Name == CommandName.GET)
            {
                return Storage.GetStorage().ExecuteGet(executableCommand);
            }else if(executableCommand.Name == CommandName.SET)
            {
                Storage.GetStorage().ExecuteSet(executableCommand);
                return "OK";
            }
            else
            {
                throw new CacheException("Command not implemented");
            }
        case CommandType.SYSTEM:
                throw new CacheException("Command not implemented");

    }
    return null;
}