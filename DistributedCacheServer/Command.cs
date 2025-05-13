using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DistributedCacheServer
{

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

    public class Command
    {
        private const int DEFAULT_EXPIRY_SECONDS = 60;

        public CommandName Name;
        public CommandType Type;
        public ValueItem Value;


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
                    command.Value.Expiry = DateTime.Now.AddSeconds(seconds);
                }
                else
                {
                    command.Value.Expiry = DateTime.Now.AddSeconds(DEFAULT_EXPIRY_SECONDS);
                }
            }
            else
            {
                throw new CacheException("Incorrect Arguements");
            }
            return command;
        }
    }
}
