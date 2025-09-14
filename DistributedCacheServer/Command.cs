using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DistributedCacheServer
{




    public class Command
    {

        public CommandName Name;
        public CommandType Type;
        public ValueItem Value;

        public enum CommandName
        {
            GET,
            SET,
            EXPIRY,
            PING
        }


        public enum CommandType
        {
            STORAGE,
            SYSTEM
        }

        public static Command Parse(object[] items)
        {
            Command command = new Command();
            
            command.Name = Enum.Parse<CommandName>(items[0].ToString(), true);

            switch (command.Name)
            {
                case CommandName.GET:
                    command.Type = CommandType.STORAGE;
                    command = ParseGET(command,items);
                    break;
                case CommandName.SET:
                    command.Type = CommandType.STORAGE;
                    command = ParseSET(command,items);
                    break;
                default:
                    throw new CacheException("Command Not Found");
            }

            return command;
        }

        public static object Execute(Command executableCommand)
        {
            switch (executableCommand.Type)
            {
                case CommandType.STORAGE:
                    if (executableCommand.Name == CommandName.GET)
                    {
                        return Storage.GetStorage().ExecuteGet(executableCommand);
                    }
                    else if (executableCommand.Name == CommandName.SET)
                    {
                        Storage.GetStorage().ExecuteSet(executableCommand);
                        return "OK";
                    }
                    else
                    {
                        throw new CacheException("Command not implemented");
                    }
                case CommandType.SYSTEM:
                    throw new CacheException("Command not implemented");

            }
            return null;
        }

        private static Command ParseGET(Command command,object[] items)
        {
            if(items.Length == 2)
            {
                command.Value = new ValueItem { Key = items[1].ToString() };
            }
            else
            {
                throw new CacheException("Incorrect Arguements");
            }
            return command;
        }

        private static Command ParseSET(Command command, object[] items)
        {
            if (items.Length >= 3)
            {
                ValueItem valueItem = new ValueItem{ Key = items[1].ToString(),Value = items[2] };
                command.Value = valueItem;

                // expiry time
                if ( items.Length == 4 && Int32.TryParse(items[3].ToString(), out int seconds))
                {
                    command.Value.Expiry = DateTimeOffset.UtcNow.AddSeconds(seconds).Ticks;
                }
                
            }
            else
            {
                throw new CacheException("Incorrect Arguements");
            }
            return command;
        }

        public override string ToString()
        {
            switch (Name)
            {
                case CommandName.GET:
                    return $"GET {Value.Key}";
                case CommandName.SET:
                    return $"SET {Value.Key} {Value.Value} {Value.Expiry}";    

                default:
                    return null;
            }
        }

    }
}
