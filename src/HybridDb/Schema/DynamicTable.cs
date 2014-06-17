using System.Collections.Generic;

namespace HybridDb.Schema
{
    public class DynamicTable : Table
    {
        public DynamicTable(string name) : base(name) {}

        public override Column this[KeyValuePair<string, object> namedValue]
        {
            get { return this[namedValue.Key] ?? new Column(namedValue.Key, namedValue.Value.GetTypeOrDefault()); }
        }
    }
}