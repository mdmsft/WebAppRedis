using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace WebAppRedis.Core.Services
{
    public class ValuesStore
    {
        private static readonly Dictionary<string, string> values = Enumerable.Range(0, byte.MaxValue + 1).ToDictionary(i => i.ToString(), i => new Guid(i, (short)i, (short)i, Enumerable.Range(0, 8).Select(_ => byte.MinValue).ToArray()).ToString());

        public Task<IEnumerable<string>> GetKeysAsync() =>
            Task.FromResult(values.Select(value => value.Key));

        public Task<Dictionary<string, string>> GetValuesAsync() =>
            Task.FromResult(values);

        public Task<string> GetValueAsync(string key) =>
            Task.FromResult(values[key]);
    }
}
