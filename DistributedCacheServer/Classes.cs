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
        public long Expiry {  get; set; }
        


        public ValueItem Clone()
        {
            return new ValueItem()
            {
                Key = this.Key, 
                Value = this.Value,
                Expiry = this.Expiry

            };
        }
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
