using Microsoft.Extensions.Configuration;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Timers;
using Timer = System.Timers.Timer;

namespace DistributedCacheServer
{
    public class Persistance : IDisposable
    {

        private static CancellationTokenSource _cts;
        
        private static ConcurrentDictionary<string, ValueItem> ValuePairsBuffer ;
        private static IConfiguration config ;
        private static string directoryPath = Path.Combine(Directory.GetCurrentDirectory(), config["RDBSnapshotDirectory"]);
        private static readonly Lazy<Persistance> _instance= new ( () => new Persistance(),LazyThreadSafetyMode.ExecutionAndPublication);

        public static Persistance Instance => _instance.Value;

        private Persistance()
        {
            config = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();
            ValuePairsBuffer = new ConcurrentDictionary<string, ValueItem>();
        }

        public enum PersistanceMode
        {
            RDB,
            AOF,
            RDB_AOF,
            NONE
        }

        public enum RecoveryMode
        {
            RDB,
            AOF,
            NONE
        }

        public void AddToBuffer(string key, ValueItem item)
        {
            ValuePairsBuffer[key] = item;
            if (item.Value != null) // Only enqueue non-deleted items for AOF
                AOFOperations.Instance.AddCommand($"SET {key} {item.Value}");
        }



        
        private static void StopPersistance()
        {
            _cts.Cancel();
        }

      
        public static void LoadStorage(RecoveryMode recoveryMode)
        {
            if (recoveryMode == RecoveryMode.RDB)
            {
                if (Directory.Exists(directoryPath))
                {
                    
                    Storage.GetStorage().LoadStorage(RDBOperations.Instance.Load());
                }
            }

            if(recoveryMode == RecoveryMode.AOF)
            {
                Storage.GetStorage().LoadStorage(AOFOperations.Instance.Load());
            }
        }

        public static void StartPersistance(PersistanceMode persistanceMode)
        {
            if (persistanceMode == PersistanceMode.RDB)
            {
                
            }
            if(persistanceMode == PersistanceMode.AOF)
            {
                AOFOperations.Instance.Start();
            }
            if(persistanceMode == PersistanceMode.RDB_AOF)
            {
                AOFOperations.Instance.Start();

            }
        }

        public void Dispose()
        {
            AOFOperations.Instance.Dispose();
            RDBOperations.Instance.Dispose();
            StopPersistance();
        }
    }
}
