using System;

namespace WebAppRedis.Core
{
    internal class AzureRedisEvent
    {
        internal AzureRedisEvent(string message)
        {
            var info = message?.Split('|');
            for (int i = 0; i < info?.Length / 2; i++)
            {
                string key = null, value = null;
                if (2 * i < info.Length)
                {
                    key = info[2 * i].Trim();
                }
                if (2 * i + 1 < info.Length)
                {
                    value = info[2 * i + 1].Trim();
                }
                if (!string.IsNullOrEmpty(key) && !string.IsNullOrEmpty(value))
                {
                    switch (key.ToLowerInvariant())
                    {
                        case "notificationtype" when value.ToLowerInvariant().Equals("nodemaintenancestarting"):
                            NotificationType = NotificationTypes.NodeMaintenanceStarting;
                            break;
                        case "notificationtype" when value.ToLowerInvariant().Equals("nodemaintenanceended"):
                            NotificationType = NotificationTypes.NodeMaintenanceEnded;
                            break;
                        case "starttimeinutc":
                            DateTimeOffset.TryParse(value, out StartTimeInUTC);
                            break;
                        case "isreplica":
                            bool.TryParse(value, out IsReplica);
                            break;
                        case "ipaddress":
                        case "sslport":
                        case "nonsslport":
                            break;
                        default:
                            Console.WriteLine($"Unexpected key {key} with value {value}");
                            break;
                    }
                }
            }
        }

        public readonly NotificationTypes NotificationType;

        public readonly DateTimeOffset StartTimeInUTC;

        public readonly bool IsReplica;
    }

    internal enum NotificationTypes
    {
        NodeMaintenanceStarting,
        NodeMaintenanceEnded
    }
}
