using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DistributedCacheServer
{
    public class Storage
    {
        private  Dictionary<string, ValueItem> ValuePairs = new Dictionary<string, ValueItem>();
        private static Storage storage = new Storage ();

        private Storage()
        {
        }
       
        public static Storage GetStorage()
        {
            if(storage == null)
            storage = new Storage();
            return storage;
        }

        public void ExecuteSet(Command command)
        {
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
    }
}
