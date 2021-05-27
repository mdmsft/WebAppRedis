using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace WebAppRedis.Core
{
    public sealed class CacheManager : IDisposable
    {
        private readonly ILogger<CacheManager> logger;

        private const string AzureRedisEventsChannel = "AzureRedisEvents";

        private long nodeMaintenance = 0;

        private bool isNodeMaintenance
        {
            get => Interlocked.Read(ref nodeMaintenance) == 1;
            set => Interlocked.Exchange(ref nodeMaintenance, value ? 1 : 0);
        }

        long lastReconnectTicks = DateTimeOffset.MinValue.UtcTicks;
        DateTimeOffset firstError = DateTimeOffset.MinValue;
        DateTimeOffset previousError = DateTimeOffset.MinValue;

        static readonly object reconnectLock = new();

        private static readonly TimeSpan ReconnectMinFrequency = TimeSpan.FromSeconds(60);

        private static readonly TimeSpan ReconnectErrorThreshold = TimeSpan.FromSeconds(30);

        private static readonly int retryMaxAttempts = 5;

        private static readonly TimeSpan masterMaintenanceOffDelay = TimeSpan.FromMinutes(5);

        private readonly string connectionString;

        private Timer masterMaintenanceTimer;

        private Timer slaveMaintenanceTimer;

        private Lazy<ConnectionMultiplexer> multiplexer;

        private IDatabase Database => multiplexer.Value.GetDatabase();

        private ISubscriber subscriber;

        public CacheManager(IConfiguration configuration, ILogger<CacheManager> logger)
        {
            this.logger = logger;

            connectionString = configuration.GetConnectionString("Redis");

            multiplexer = CreateMultiplexer();
        }

        private void MaintenanceCallback(object state)
        {
            if (state is bool maintenance && isNodeMaintenance != maintenance)
            {
                logger.LogInformation("Switching node maintenance {0}", maintenance ? "on" : "off");
                isNodeMaintenance = maintenance;
            }
        }

        public async Task StringSetAsync(RedisKey key, RedisValue value)
        {
            logger.LogDebug("[{operation}] {key}={value}", nameof(StringSetAsync), key, value);
            if (isNodeMaintenance)
            {
                logger.LogDebug("Skipping due to node maintenance");
            }
            else
            {
                await Database.StringSetAsync(key, value);
            }
        }

        public async Task StringSetAsync(KeyValuePair<RedisKey, RedisValue>[] keyValuePairs)
        {
            logger.LogDebug("[{operation}] with {length} key-value pairs", nameof(StringSetAsync), keyValuePairs.Length);
            if (isNodeMaintenance)
            {
                logger.LogDebug("Skipping due to node maintenance");
            }
            else
            {
                await Database.StringSetAsync(keyValuePairs);
            }
        }

        public async Task<RedisValue> StringGetAsync(RedisKey key)
        {
            logger.LogDebug("[{operation}] {key}", nameof(StringGetAsync), key);
            if (isNodeMaintenance)
            {
                logger.LogDebug("Skipping due to node maintenance");
                return default;
            }
            else
            {
                return await StringGetAsyncWithRetry(key);
            }
        }

        public async Task<RedisValue[]> StringGetAsync(RedisKey[] keys)
        {
            logger.LogDebug("[{operation}] with {length} keys", nameof(StringGetAsync), keys.Length);
            if (isNodeMaintenance)
            {
                logger.LogDebug("Skipping due to node maintenance");
                return default;
            }
            else
            {
                return await Database.StringGetAsync(keys);
            }
        }

        public void Dispose()
        {
            UnsubscribeFromAzureRedisEvents();
            masterMaintenanceTimer?.Dispose();
            slaveMaintenanceTimer?.Dispose();
        }

        private void AzureRedisEventHandler(RedisChannel channel, RedisValue value)
        {
            logger.LogInformation("{channel}: {message}", channel, value);
            var azureRedisEvent = new AzureRedisEvent(value);
            if (azureRedisEvent.NotificationType == NotificationTypes.NodeMaintenanceStarting)
            {
                logger.LogInformation("Node maintenance scheduled for {timestamp:G} UTC", azureRedisEvent.StartTimeInUTC);
                var delay = DateTimeOffset.UtcNow.Subtract(azureRedisEvent.StartTimeInUTC).Subtract(TimeSpan.FromSeconds(1));
                if (delay > TimeSpan.Zero)
                {
                    slaveMaintenanceTimer?.Dispose();
                    slaveMaintenanceTimer = new Timer(MaintenanceCallback, true, delay, Timeout.InfiniteTimeSpan);
                    logger.LogInformation("Scheduled node maintenance toggle switch in {0}", delay);

                    if (!azureRedisEvent.IsReplica)
                    {
                        masterMaintenanceTimer?.Dispose();
                        masterMaintenanceTimer = new Timer(MaintenanceCallback, false, delay.Add(masterMaintenanceOffDelay), Timeout.InfiniteTimeSpan);
                        logger.LogInformation("Scheduled master maintenance toggle switch in {0}", delay.Add(masterMaintenanceOffDelay));
                    }
                }
                else
                {
                    logger.LogWarning("Skipping node maintenance schedule due to negative delay {0}", delay);
                }
                
            }
            else if (azureRedisEvent.NotificationType == NotificationTypes.NodeMaintenanceEnded)
            {
                logger.LogInformation("Node maintenance ended, switching off node maintenance toggle");
                MaintenanceCallback(false);
            }
        }

        private Lazy<ConnectionMultiplexer> CreateMultiplexer() =>
            new(() =>
            {
                var configurationOptions = ConfigurationOptions.Parse(connectionString);
                configurationOptions.ConfigCheckSeconds = 1;
                var multiplexer = ConnectionMultiplexer.Connect(configurationOptions);
                SubcribeToAzureRedisEvents(multiplexer);
                return multiplexer;
            });

        private void SubcribeToAzureRedisEvents(IConnectionMultiplexer multiplexer)
        {
            subscriber = multiplexer.GetSubscriber();
            subscriber.Subscribe(AzureRedisEventsChannel, AzureRedisEventHandler);
        }

        private void UnsubscribeFromAzureRedisEvents()
        {
            subscriber.Unsubscribe(AzureRedisEventsChannel);
        }

        private void CloseMultiplexer(Lazy<ConnectionMultiplexer> multiplexer)
        {
            if (multiplexer is not null)
            {
                try
                {
                    UnsubscribeFromAzureRedisEvents();
                    multiplexer.Value.Close();
                }
                catch (Exception exception)
                {
                    logger.LogError(exception, "Could not close old multiplexer");
                }
            }
        }

        private void Reconnect()
        {
            var utcNow = DateTimeOffset.UtcNow;
            var previousTicks = Interlocked.Read(ref lastReconnectTicks);
            var previousReconnect = new DateTimeOffset(previousTicks, TimeSpan.Zero);
            var elapsedSinceLastReconnect = utcNow - previousReconnect;

            if (elapsedSinceLastReconnect > ReconnectMinFrequency)
            {
                lock (reconnectLock)
                {
                    utcNow = DateTimeOffset.UtcNow;
                    elapsedSinceLastReconnect = utcNow - previousReconnect;

                    if (firstError == DateTimeOffset.MinValue)
                    {
                        firstError = utcNow;
                        previousError = utcNow;
                        return;
                    }

                    if (elapsedSinceLastReconnect < ReconnectMinFrequency)
                    {
                        return;
                    }

                    var elapsedSinceFirstError = utcNow - firstError;
                    var elapsedSinceMostRecentError = utcNow - previousError;

                    var shouldReconnect = elapsedSinceFirstError >= ReconnectErrorThreshold && elapsedSinceMostRecentError <= ReconnectErrorThreshold;

                    previousError = utcNow;

                    if (shouldReconnect)
                    {
                        firstError = DateTimeOffset.MinValue;
                        previousError = DateTimeOffset.MinValue;

                        var _multiplexer = multiplexer;
                        CloseMultiplexer(_multiplexer);
                        multiplexer = CreateMultiplexer();
                        Interlocked.Exchange(ref lastReconnectTicks, utcNow.UtcTicks);
                    }
                }
            }
        }

        private async Task<RedisValue> StringGetAsyncWithRetry(RedisKey key)
        {
            int reconnectRetry = 0;
            int disposedRetry = 0;

            while (true)
            {
                try
                {
                    return await Database.StringGetAsync(key);
                }
                catch (Exception exception) when (exception is RedisConnectionException or SocketException)
                {
                    reconnectRetry++;
                    logger.LogError(exception, "Retrying reconnect {attempt}/{attempts}...", reconnectRetry, retryMaxAttempts);
                    if (reconnectRetry > retryMaxAttempts)
                    {
                        logger.LogError(exception, "Max retry attempts reached, giving up");
                        throw;
                    }
                    Reconnect();
                }
                catch (ObjectDisposedException)
                {
                    disposedRetry++;
                    logger.LogWarning("Retrying on disposed object {attempt}/{attempts}...", reconnectRetry, retryMaxAttempts);
                    if (disposedRetry > retryMaxAttempts)
                    {
                        logger.LogWarning("Max retry attempts reached, giving up");
                        throw;
                    }
                }
            }
        }
    }
}
