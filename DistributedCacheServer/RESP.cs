using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DistributedCacheServer
{
    public class RESP
    {
        private const string CrLf = "\r\n";

        public static byte[] Serialize(object input)
        {
            
            string command;
            if (input is object[])
            {
                command = SerializeArray(input as object[]);
            }
            else if (input is string)
            {
                command = SerializeString(input.ToString());
            }
            else if (input is Int32)
            {
                if (-1 == (int)input)
                    command = SerializeNull();
                else
                    command = SerializeInt(Convert.ToInt32(input));
            }
            else
            {
                throw new CacheException("Datatype Not Supported");
            }

            byte[] bytes = Encoding.UTF8.GetBytes(command);

            return bytes;
        }

        private static string SerializeNull()
        {
            return $"_{CrLf}";

        }

        private static string SerializeError(int v)
        {
            return $"-{v}{CrLf}";
        }

        private static string SerializeString(string input)
        {
            return $"+{input}{CrLf}";
        }

        private static string SerializeInt(int input)
        {
            return $":{input}{CrLf}";
        }

        private static string SerializeBulkString(object input)
        {
            int len = Encoding.UTF8.GetByteCount(input.ToString());
            return $"${len}{CrLf}{input}{CrLf}";
        }

        private static string SerializeArray(object[] input)
        {
            int len = input.Length;
            StringBuilder sb = new StringBuilder();
            sb.Append($"*{len}{CrLf}");
            foreach (var item in input)
            {
                sb.Append(SerializeBulkString(item));
            }
            return sb.ToString();
        }


        public static List<List<string>> DeseerializeBulk(byte[] bytes)
        {
            List<List<string>> commands = new List<List<string>>();
            var commandsStr = Encoding.UTF8.GetString(bytes);
            int pos = 0;
            var current = commandsStr[0];
            while(pos < commandsStr.Length)
            {
                pos++;
                if(current == '*')
                {
                    commands.Add(DeserializeArray(ref pos, commandsStr));
                }
                else
                {
                    throw new Exception("File corrupted: expected char '*'");
                }
            }
            return commands;

        }

        public static List<object> Deserialize(byte[] bytes)
        {

            var obj = DeserializeObj(bytes);
            return obj;
        }
      

        private static List<object> DeserializeObj(byte[] bytes)
        {
            int pos = 0;
            List<object> args = new List<object>();
            string response = Encoding.UTF8.GetString(bytes);
            while (pos < response.Length)
            {
                char start = response[pos];
                if (start == '+' || start == '-')
                {
                    pos++;
                    object value = DeserializeString(pos, response);
                    if (value != null) args.Add(value);
                }
                else if (start == ':')
                {
                    pos++;
                    object value = DeserializeInt(pos, response);
                    if (value != null) args.Add(value);

                }
                else if (start == '$')
                {
                    pos++;
                    object value = DeserializeBulkString(ref pos, response);
                    if (value != null) args.Add(value);
                }
                else if (start == '*')
                {
                    // array
                    pos++;
                    List<string> values = DeserializeArray(ref pos, response);
                    args.AddRange(values);
                }

                else
                {
                    throw new Exception("Unknown RESP format");
                }

                if (args.Count == 0)
                    args.Add("(nil)");

                return args;
            }
            throw new Exception("Unexpected end of data");
        }

        private static List<string> DeserializeArray(ref int pos, string response)
        {
            int end = response.IndexOf(CrLf, pos);
            string lengthStr = response.Substring(pos, end - pos);
            long length = long.Parse(lengthStr);
            List<string> values = new List<string>();
            pos += lengthStr.Length + CrLf.Length;
            for (int i = 0; i < length; i++)
            {
                // read each bulk string
                end = response.IndexOf(CrLf, pos);

                pos++; // increment pos becauuse want to skip prefix '$'
                string bulkLengthStr = response.Substring(pos, end - pos);
                int bulkLength = int.Parse(bulkLengthStr);
                pos += bulkLengthStr.Length + CrLf.Length;
                string value = response.Substring(pos, bulkLength);
                values.Add(value);
                pos += bulkLength + CrLf.Length;
            }

            return values;
        }

        private static object DeserializeBulkString(ref int pos, string response)
        {
            int end = response.IndexOf(CrLf, pos);
            string lengthStr = response.Substring(pos, end - pos);
            int length = int.Parse(lengthStr);
            pos += lengthStr.Length + CrLf.Length;
            string value = response.Substring(pos, length);

            return value;
        }

        private static object DeserializeString(int pos, string response)
        {
            int end = response.IndexOf(CrLf, pos);
            string value = response.Substring(pos, end - pos);
            return value;
        }

        private static object DeserializeInt(int pos, string response)
        {
            int end = response.IndexOf(CrLf, pos);
            string value = response.Substring(pos, end - pos);
            return int.Parse(value);
        }
    }
}
