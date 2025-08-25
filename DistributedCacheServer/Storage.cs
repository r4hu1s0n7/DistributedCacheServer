using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DistributedCacheServer
{
    public class Storage
    {
        private  ConcurrentDictionary<string, ValueItem> ValuePairs = new ConcurrentDictionary<string, ValueItem>();
        private static Storage storage = new Storage ();
        private IConfiguration config = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();

        private readonly int DEFAULT_EXPIRY_SECONDS;

        private Storage()
        {
            DEFAULT_EXPIRY_SECONDS = Convert.ToInt32(config["DefaultExpiryTime"]);
        }
       
        public static Storage GetStorage()
        {
            if(storage == null)
            storage = new Storage();
            return storage;
        }

        public void ExecuteSet(Command command)
        {
            if(command.Value.Expiry == null)
            {
                command.Value.Expiry = DateTime.Now.AddSeconds(DEFAULT_EXPIRY_SECONDS);    
            }
            ValuePairs[command.Value.Key]= command.Value;
        }

        public object ExecuteGet(Command command)
        {
            string key = command.Value.Key;
            if (ValuePairs.ContainsKey(key))
            {
                if (ValuePairs[key].Expiry > DateTime.Now)
                    return ValuePairs[key].Value;
                else
                    return -1;
            }
            else
            {
                return -1;
            }
        }

        public Dictionary<string,ValueItem> CopyStorage()
        {
            // make deep copy ... because windows doesn't support fork
            return ValuePairs.ToDictionary(
                entry => entry.Key,
                entry => entry.Value.Clone()
                );
        }

        public void LoadStorage(ConcurrentDictionary<string, ValueItem> pairs)
        {
            ValuePairs = pairs;
        }
    }
}
