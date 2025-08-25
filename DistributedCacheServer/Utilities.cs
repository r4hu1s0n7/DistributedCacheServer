using System.IO.Hashing;

namespace DistributedCacheServer
{
    public class Utilities
    {
        public static byte[] GetCRCHash(byte[] data)
        {
            return Crc32.Hash(data);
        }
    }
    
}
