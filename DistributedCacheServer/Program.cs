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
if (Enum.TryParse<Persistance.RecoveryMode>(config["RecoveryMode"], true, out var RecoveryMode))
{
    Persistance.LoadStorage(RecoveryMode);
}


if (Enum.TryParse<Persistance.PersistanceMode>(config["PersistanceMode"], true,out var PersistanceMode))
{        
    Persistance.StartPersistance(PersistanceMode);
}





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