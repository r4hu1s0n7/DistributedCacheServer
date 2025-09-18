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

        #region Serialization Methods
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
            if (input == null)
                return $"$-1{CrLf}";

            int len = Encoding.UTF8.GetByteCount(input.ToString());
            return $"${len}{CrLf}{input}{CrLf}";
        }
        private static string SerializeError(string message)
        {
            return $"-ERR {message}{CrLf}";
        }

        private static string SerializeArray(object[] input)
        {
            if (input == null)
                return $"*-1{CrLf}";

            int len = input.Length;
            StringBuilder sb = new StringBuilder();
            sb.Append($"*{len}{CrLf}");
            foreach (var item in input)
            {
                sb.Append(SerializeBulkString(item));
            }
            return sb.ToString();
        }

        public static byte[] SerializeBatchResponse(object[] responses)
        {
            var responseStrings = new List<string>();

            foreach (var response in responses)
            {
                if (response is string str)
                    responseStrings.Add(SerializeString(str));
                else if (response is int intVal)
                    responseStrings.Add(SerializeInt(intVal));
                else if (response == null)
                    responseStrings.Add(SerializeNull());
                else
                    responseStrings.Add(SerializeBulkString(response.ToString()));
            }

            return Encoding.UTF8.GetBytes(string.Join("", responseStrings));
        }

        #endregion

        public static List<List<string>> DeseerializeBulk(byte[] bytes)
        {
            List<List<string>> commands = new List<List<string>>();
            var commandsStr = Encoding.UTF8.GetString(bytes);
            int pos = 0;
            var current = commandsStr[0];
            while (pos < commandsStr.Length)
            {
                pos++;
                if (current == '*')
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


       




        private static List<string> ParseSingleCommand(string data, ref int pos)
        {
            if (pos >= data.Length || data[pos] != '*')
                return null;

            pos++;  // Skip '*'

            int crlfPos = data.IndexOf(CrLf, pos);
            if (crlfPos == -1)
                throw new InvalidOperationException("Invalid RESP format: missing CRLF after array length");

            string lengthStr = data.Substring(pos, crlfPos - pos);
            if (!int.TryParse(lengthStr, out int arrayLength))
                throw new InvalidOperationException($"Invalid array length: {lengthStr}");

            pos = crlfPos + CrLf.Length;
            var command = new List<string>();

            // Read each bulk string in the array
            for (int i = 0; i < arrayLength; i++)
            {
                string bulkString = ParseBulkString(data, ref pos);
                command.Add(bulkString);
            }

            return command;

        }

        public static List<List<string>> ParseBatchCommands(byte[] bytes)
        {
            var commands = new List<List<string>>();
            var data = Encoding.UTF8.GetString(bytes);
            int pos = 0;

            while (pos < data.Length)
            {
                // Skip any whitespace or empty lines
                while (pos < data.Length && char.IsWhiteSpace(data[pos]) && data[pos] != '*')
                {
                    pos++;
                }

                if (pos >= data.Length)
                    break;

                // Each command should start with '*' (array)
                if (data[pos] != '*')
                {
                    throw new InvalidOperationException($"Expected '*' at position {pos}, got '{data[pos]}'");
                }

                var command = ParseSingleCommand(data, ref pos);
                if (command != null && command.Count > 0)
                {
                    commands.Add(command);
                }
            }

            return commands;
        }

        private static string ParseBulkString(string data, ref int pos)
        {
            if (pos >= data.Length || data[pos] != '$')
                throw new InvalidOperationException($"Expected '$' at position {pos}");

            pos++;
            int crlfPos = data.IndexOf(CrLf, pos);

            if (crlfPos == -1)
                throw new InvalidOperationException("Invalid RESP format: missing CRLF after bulk string length");

            string lengthStr = data.Substring(pos, crlfPos - pos);
            if (!int.TryParse(lengthStr, out int bulkLength))
                throw new InvalidOperationException($"Invalid bulk string length: {lengthStr}");

            pos = crlfPos + CrLf.Length;

            if (bulkLength == -1) return null;

            if (bulkLength == 0)                      // Empty bulk string
            {
                if (pos + 1 < data.Length && data.Substring(pos, 2) == CrLf)
                    pos += 2;
                return string.Empty;
            }

            if (pos + bulkLength > data.Length)
                throw new InvalidOperationException("Invalid RESP format: bulk string length exceeds available data");

            string value = data.Substring(pos, bulkLength);
            pos += bulkLength;

            if (pos + 1 < data.Length && data.Substring(pos, 2) == CrLf)
                pos += 2;

            return value;
        }


        #region Deserialization Methods

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

        private static int DeserializeInteger(string data, ref int pos)
        {
            if (pos >= data.Length || data[pos] != ':')
                throw new InvalidOperationException($"Expected ':' at position {pos}");

            pos++; // Skip ':'

            int crlfPos = data.IndexOf(CrLf, pos);
            if (crlfPos == -1)
                throw new InvalidOperationException($"Missing CRLF after integer at position {pos}");

            string intStr = data.Substring(pos, crlfPos - pos);
            if (!int.TryParse(intStr, out int value))
                throw new InvalidOperationException($"Invalid integer '{intStr}' at position {pos}");

            pos = crlfPos + CrLf.Length;
            return value;
        }

        public static List<List<string>> DeserializeBulkCommands(byte[] bytes)
        {
            if (bytes == null || bytes.Length == 0)
                return new List<List<string>>();

            var commands = new List<List<string>>();
            var data = Encoding.UTF8.GetString(bytes);
            int pos = 0;
            try
            {
                while (pos < data.Length)
                {
                    while (pos < data.Length && (data[pos] == ' ' || data[pos] == '\t'))
                    {
                        pos++;
                    }

                    if (pos >= data.Length)
                        break;

                    char currentChar = data[pos];

                    switch (currentChar)
                    {
                        case '*': // array
                            var arrayCommand = DeserializeArrayCommand(data, ref pos);
                            if (arrayCommand != null && arrayCommand.Count > 0)
                                commands.Add(arrayCommand);
                            break;

                        case '$': // bulk string
                            var bulkString = ParseBulkString(data, ref pos);
                            if (bulkString != null)
                                commands.Add(new List<string> { bulkString });
                            break;

                        case '+': // Simple string 
                            var simpleString = DeserializeSimpleString(data, ref pos);
                            if (simpleString != null)
                                commands.Add(new List<string> { simpleString });
                            break;

                        case ':': // Integer 
                            var integer = DeserializeInteger(data, ref pos);
                            commands.Add(new List<string> { integer.ToString() });
                            break;

                        case '-': // Error 
                            var error = DeserializeError(data, ref pos);
                            commands.Add(new List<string> { "ERROR", error });
                            break;

                        case '\r':
                        case '\n':
                            // Skip CRLF
                            pos++;
                            break;
                        default:
                            throw new InvalidOperationException(
                                $"Unknown RESP type '{currentChar}' at position {pos}. " +
                                $"Context: '{GetContextString(data, pos)}'");
                    }
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Failed to deserialize bulk commands at position {pos}. " +
                    $"Context: '{GetContextString(data, pos)}'. Error: {ex.Message}", ex);
            }

            return commands;

        }

        private static List<string> DeserializeArrayCommand(string data, ref int pos)
        {
            if (pos >= data.Length || data[pos] != '*')
                return null;

            pos++; // Skip '*'

            // Read array length
            int crlfPos = data.IndexOf(CrLf, pos);
            if (crlfPos == -1)
                throw new InvalidOperationException($"Missing CRLF after array length at position {pos}");

            string lengthStr = data.Substring(pos, crlfPos - pos);
            if (!int.TryParse(lengthStr, out int arrayLength))
                throw new InvalidOperationException($"Invalid array length '{lengthStr}' at position {pos}");

            pos = crlfPos + CrLf.Length;

            // Handle special array lengths
            if (arrayLength == -1)
                return null; // Null array

            if (arrayLength == 0)
                return new List<string>(); // Empty array

            var command = new List<string>();

            // Read each element in the array
            for (int i = 0; i < arrayLength; i++)
            {
                if (pos >= data.Length)
                    throw new InvalidOperationException($"Unexpected end of data while reading array element {i + 1}");

                char elementType = data[pos];
                string element;

                switch (elementType)
                {
                    case '$': // Bulk string (most common)
                        element = ParseBulkString(data, ref pos);
                        break;

                    case '+': // Simple string
                        element = DeserializeSimpleString(data, ref pos);
                        break;

                    case ':': // Integer
                        var intVal = DeserializeInteger(data, ref pos);
                        element = intVal.ToString();
                        break;

                    case '-': // Error
                        element = DeserializeError(data, ref pos);
                        break;

                    case '*': // Nested array (rare but possible)
                        var nestedArray = DeserializeArrayCommand(data, ref pos);
                        element = nestedArray != null ? $"[{string.Join(",", nestedArray)}]" : "null";
                        break;

                    default:
                        throw new InvalidOperationException(
                            $"Invalid array element type '{elementType}' at position {pos} " +
                            $"(element {i + 1} of {arrayLength})");
                }

                command.Add(element);
            }

            return command;
        }

        private static string DeserializeError(string data, ref int pos)
        {
            if (pos >= data.Length || data[pos] != '-')
                throw new InvalidOperationException($"Expected '-' at position {pos}");

            pos++; // Skip '-'

            int crlfPos = data.IndexOf(CrLf, pos);
            if (crlfPos == -1)
                throw new InvalidOperationException($"Missing CRLF after error at position {pos}");

            string error = data.Substring(pos, crlfPos - pos);
            pos = crlfPos + CrLf.Length;

            return error;
        }


        private static string DeserializeSimpleString(string data, ref int pos)
        {
            if (pos >= data.Length || data[pos] != '+')
                throw new InvalidOperationException($"Expected '+' at position {pos}");

            pos++; // Skip '+'

            int crlfPos = data.IndexOf(CrLf, pos);
            if (crlfPos == -1)
                throw new InvalidOperationException($"Missing CRLF after simple string at position {pos}");

            string value = data.Substring(pos, crlfPos - pos);
            pos = crlfPos + CrLf.Length;

            return value;
        }

        private static string GetContextString(string data, int pos)
        {
            int start = Math.Max(0, pos - 10);
            int end = Math.Min(data.Length, pos + 10);
            int relativePos = pos - start;

            string context = data.Substring(start, end - start)
                .Replace("\r", "\\r")
                .Replace("\n", "\\n");

            return $"{context.Substring(0, relativePos)}[HERE]{context.Substring(relativePos)}";
        }

        #endregion

    }


}
