using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DistributedCacheServer
{
    public  class RDBOperations : IDisposable
    {
        private static IConfiguration config ;
        private static string directoryPath;
        private  CancellationTokenSource _cts;
        private static readonly Lazy<RDBOperations> _instance = new ( () => new RDBOperations(), LazyThreadSafetyMode.ExecutionAndPublication);

        public static RDBOperations Instance => _instance.Value;

        public RDBOperations()
        {
            config = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();
            directoryPath = Path.Combine(Directory.GetCurrentDirectory(), config["RDBSnapshotDirectory"]);
        }

        public void SaveRDBSnapshot(Dictionary<string, ValueItem> ValuePairsBuffer)
        {

            if (!Directory.Exists(directoryPath)) { Directory.CreateDirectory(directoryPath); }
            string filename = $"RDB_{DateTime.Now.ToString("ddMMyyyy_HHmmss")}";
            string filePath = Path.Combine(directoryPath, filename);

            using var fs = new FileStream(filePath, FileMode.Create);
            using var bw = new BinaryWriter(fs);

            long startPos = fs.Position;


            bw.Write(Encoding.ASCII.GetBytes("RDB"));
            bw.Write(ValuePairsBuffer.Count);
             
            foreach (var pair in ValuePairsBuffer)
            {
                bw.Write(pair.Key);
                bw.Write(pair.Value.Key);
                bw.Write(pair.Value.Value?.ToString() ?? "");
                bw.Write(pair.Value.Expiry);
                
            }

            // Generate Hash 
            long endPos = fs.Position;
            fs.Seek(startPos, SeekOrigin.Begin);
            long length = endPos - startPos;
            byte[] hashBuffer = new byte[length];

            fs.Read(hashBuffer, 0, hashBuffer.Length);
            var checksumHash = Utilities.GetCRCHash(hashBuffer);

            fs.Seek(0, SeekOrigin.End);
            bw.Write(checksumHash);
            
        }

        public  ConcurrentDictionary<string, ValueItem> Load()
        {
            var latestFilename = new DirectoryInfo(directoryPath)
                        .GetFiles()
                        .OrderByDescending(f => f.LastWriteTime)
                        .FirstOrDefault().Name;
            if (string.IsNullOrEmpty(latestFilename)) throw new Exception("No File Found");
            return LoadRDBSnapshot(latestFilename);
        }

        private static ConcurrentDictionary<string,ValueItem> LoadRDBSnapshot(string latestFilename)
        {
            ConcurrentDictionary<string, ValueItem> ValuePairsBuffer = new ConcurrentDictionary<string, ValueItem>();

            

            if (string.IsNullOrEmpty(latestFilename))
            {
                throw new Exception("RDB File Not Found");
            }

            string filePath = Path.Combine(directoryPath, latestFilename);

           

            var filebytes = File.ReadAllBytes(filePath);
            if(filebytes.Length < 7)
            {
                throw new Exception("RDB File too small");
            }


            using var fs = new FileStream(filePath, FileMode.Open);
            using var br = new BinaryReader(fs);

            byte[] storedChecksum = new byte[4];
            Array.Copy(filebytes, filebytes.Length - 4, storedChecksum, 0, 4);


            byte[] data = new byte[filebytes.Length - 4];
            Array.Copy(filebytes, 0, data, 0, data.Length);
            byte[] computedChecksum = Utilities.GetCRCHash(data);

            if (!storedChecksum.SequenceEqual(computedChecksum))
            {
                throw new Exception("Checksum failed: file corrupted");
            }

            byte[] header = br.ReadBytes(3);
            if (Encoding.ASCII.GetString(header) != "RDB")
            {
                throw new Exception("Invalid RDB header");
            }

            int count = br.ReadInt32();
            ValuePairsBuffer.Clear();

            for (int i = 0; i < count; i++)
            {
                string dictKey = br.ReadString();
                string itemKey = br.ReadString();
                string valueStr = br.ReadString();  
                long ticks = br.ReadInt64();

                ValuePairsBuffer[dictKey] = new ValueItem
                {
                    Key = itemKey,
                    Value = valueStr, // stored as string — can be casted later
                    Expiry = ticks
                };
            }



            return ValuePairsBuffer;
        }

         

        public  void Start()
        {
            _cts = new CancellationTokenSource();

            int duration = Convert.ToInt32(config["RDBSnapshotInterval"]);
            Task.Run(async () => {

                while (!_cts.Token.IsCancellationRequested)
                {

                    try
                    {
                        var copy = Storage.GetStorage().CopyStorage();
                        SaveRDBSnapshot(copy);
                        RemoveOldestFile();


                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"RDB Write Failed: {e.Message}");
                    }
                    await Task.Delay(TimeSpan.FromMinutes(duration), _cts.Token);
                }
            });


        }

        private static void RemoveOldestFile()
        {
            int totalFilesCount = new DirectoryInfo(directoryPath)
                        .GetFiles().Count();

            if (totalFilesCount > Convert.ToInt32(config["RDBSnapshotVersionCount"]))
            {
                var oldestFilename = new DirectoryInfo(directoryPath)
                            .GetFiles()
                            .OrderBy(f => f.LastWriteTime)
                            .FirstOrDefault().Name;

                string oldestFilePath = Path.Combine(directoryPath, config["RDBSnapshotDirectory"]);
                if (File.Exists(oldestFilename)) File.Delete(oldestFilename);
            }
        }

        public void Stop()
        {
            _cts?.Dispose();   
        }

        public void Dispose()
        {
            Stop();
        }
    }
}
