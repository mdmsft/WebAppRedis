using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WebAppRedis.Core.Services;

namespace WebAppRedis.Core.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class ValuesController : ControllerBase
    {
        private readonly ILogger<ValuesController> logger;
        private readonly ValuesStore store;
        private readonly CacheManager manager;

        public ValuesController(ILogger<ValuesController> logger, ValuesStore store, CacheManager manager)
        {
            this.logger = logger;
            this.store = store;
            this.manager = manager;
        }

        [HttpGet]
        public async Task<string> Get()
        {
            var sb = new StringBuilder();
            var keys = await store.GetKeysAsync();
            sb.AppendJoin(Environment.NewLine, await manager.StringGetAsync(keys.Select(key => new RedisKey(key)).ToArray()));
            return sb.ToString();
        }

        [HttpGet("{id:int:min(0):max(255)}")]
        public async Task<string> Get(string id)
        {
            logger.LogInformation("Fetching value for {ID} from cache", id);
            var value = await manager.StringGetAsync(id);
            if (!value.HasValue)
            {
                logger.LogInformation("Cache miss for {ID}, retrieving from store", id);
                value = await store.GetValueAsync(id);
                await manager.StringSetAsync(id, value);
            }
            return value;
        }

        [HttpPost]
        public async Task Post()
        {
            logger.LogInformation("Posting all values to cache");
            var values = await store.GetValuesAsync();
            //var batch = database.CreateBatch();
            //foreach (var value in values)
            //{
            //    await batch.StringSetAsync(value.Key, value.Value);
            //}
            //batch.Execute();
            await manager.StringSetAsync(values.Select(value => new KeyValuePair<RedisKey, RedisValue>(value.Key, value.Value)).ToArray());
        }

        [HttpGet("verify")]
        public async Task Verify()
        {
            var values = await store.GetValuesAsync();
            foreach (var value in values)
            {
                var expectedValue = value.Value;
                var actualValue = await manager.StringGetAsync(value.Key);
                if (!actualValue.Equals(expectedValue))
                {
                    logger.LogWarning("Key mismatch for {key}: expected: {expected}, actual: {actual}", value.Key, expectedValue, actualValue);
                }
            }
        }
    }
}
