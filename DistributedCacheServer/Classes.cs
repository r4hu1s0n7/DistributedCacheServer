using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DistributedCacheServer
{
    public class ValueItem
    {
        public string Key { get; set; }
        public object Value { get; set; }
        public DateTime Expiry {  get; set; }
    }



    public class CacheException : Exception
    {
        public string Message {  get; private set; }
        public CacheException(string Message)
        {
            this.Message = $"-ERR {Message}";
        }
    }
}
