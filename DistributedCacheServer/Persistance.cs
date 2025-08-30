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
        private static string directoryPath;
        private static readonly Lazy<Persistance> _instance= new ( () => new Persistance(),LazyThreadSafetyMode.ExecutionAndPublication);
        private RecoveryMode Recovery = RecoveryMode.NONE;
        private PersistanceMode Mode = PersistanceMode.NONE;
        public static Persistance Instance => _instance.Value;

        private Persistance()
        {
            config = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();
            directoryPath = Path.Combine(Directory.GetCurrentDirectory(), config["RDBSnapshotDirectory"]);
            ValuePairsBuffer = new ConcurrentDictionary<string, ValueItem>();
            if (Enum.TryParse<Persistance.RecoveryMode>(config["RecoveryMode"], true, out var RecoveryMode))
            {
                Recovery = RecoveryMode;
            }
            else
            {
                throw new Exception("Cannot read [RecoveryMode] Config");
            }
            if (Enum.TryParse<Persistance.PersistanceMode>(config["PersistanceMode"], true, out var PersistanceMode))
            {
                Mode = PersistanceMode;

            }
            else
            {
                throw new Exception("Cannot read [PersistanceMode] Config");

            }
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

  

        
        private void StopPersistance()
        {
            _cts.Cancel();
        }

        
        public void LoadStorage()
        {
            if (Recovery == RecoveryMode.RDB)
            {
                if (Directory.Exists(directoryPath))
                {
                    
                    Storage.GetStorage().LoadStorage(RDBOperations.Instance.Load());
                }
            }

            if(Recovery == RecoveryMode.AOF)
            {
                //Storage.GetStorage().LoadStorage(AOFOperations.Instance.Load());
                var commandArgsList = AOFOperations.Instance.Load();
                foreach(var commandArgs in commandArgsList)
                {
                    var command = Command.Parse(commandArgs.ToArray());
                    Command.Execute(command);
                }
            }
        }

        public void Store(Command command)
        {
            if(Mode == PersistanceMode.AOF)
            {
                AOFOperations.Instance.AddCommand(command);
            }
        }



        public void StartPersistance()
        {
            if (Mode == PersistanceMode.RDB)
            {
                RDBOperations.Instance.Start();   
            }
            if(Mode == PersistanceMode.AOF)
            {
                AOFOperations.Instance.Start();
            }
            if(Mode == PersistanceMode.RDB_AOF)
            {
                RDBOperations.Instance.Start();   
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
