using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DistributedCacheServer
{
    public class AOFOperations : IDisposable
    {
        private static IConfiguration config;
        private static string directoryPath,filePath;
        private static readonly Lazy<AOFOperations> _instance = new(() => new AOFOperations(), LazyThreadSafetyMode.ExecutionAndPublication);
        private readonly ConcurrentQueue<string> CommandsQueue = new();
        private CancellationTokenSource _cts;
        private FileStream fileStream;
        private StreamWriter streamWriter;
        public static AOFOperations Instance => _instance.Value;

        private AOFOperations()
        {
            config = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();
            directoryPath = Path.Combine(Directory.GetCurrentDirectory(), config["AOFSnapshotDirectory"]);

            if (!Directory.Exists(directoryPath)) Directory.CreateDirectory(directoryPath);
            filePath = Path.Combine(directoryPath, "AOF.txt");


        }

        public void AddCommand(Command command)
        {
            string commandString = command.ToString();
            var commandResp = RESP.Serialize(commandString);
            var commandBytes = Encoding.UTF8.GetString(commandResp);
            CommandsQueue.Enqueue(commandBytes);
        }

        public void Start()
        {
            if (!File.Exists(filePath)) File.Create(filePath).Dispose();

            fileStream = new FileStream(filePath, FileMode.Append, FileAccess.Write, FileShare.Read, 4096, true);
            streamWriter = new StreamWriter(fileStream);
            _cts = new CancellationTokenSource();
            Task.Run(async () =>
            {
                while (!_cts.IsCancellationRequested)
                {
                    while(CommandsQueue.TryDequeue(out var command))
                    {
                        try {
                            await streamWriter.WriteLineAsync(command.Trim());
                            await streamWriter.FlushAsync();
                        }
                        catch( Exception e)
                        {
                            Console.WriteLine($"Error in AOF operation:{e.Message}");
                        }
                    }
                }
            });
        }

     
        public void Stop()
        {
            _cts?.Cancel();
            _cts?.Dispose();
        }

        public List<List<string>> Load()
        {
            if (!File.Exists(filePath)) throw new Exception($"File not found: {filePath}");
            var filebytes = File.ReadAllBytes(filePath);
           
            var commands = RESP.DeseerializeBulk(filebytes);
            return commands;
        }

        public void Dispose()
        {
            Stop();
            fileStream?.Dispose();
            streamWriter?.Dispose();
            
        }
    }
}
