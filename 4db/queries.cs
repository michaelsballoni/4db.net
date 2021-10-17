using System;
using System.Collections.Generic;

namespace fourdb
{
    public class Define
    {
        public string table { get; set; }
        public object key { get; set; }
        public Dictionary<string, object> metadata { get; set; } = new Dictionary<string, object>();

        public Define() { }

        public Define(string table, object key)
        {
            this.table = table;
            this.key = key;
        }

        public Define Set(string metadataName, object metadataValue)
        {
            metadata[metadataName] = metadataValue;
            return this;
        }

        public object this[string name]
        {
            get { return metadata[name]; }
            set { metadata[name] = value; }
        }
    }

    public class GetRequest
    {
        public string table { get; set; }
        public List<object> values { get; set; }
    }

    public class Criteria // WHERE
    {
        public string name { get; set; }
        public string op { get; set; }
        public string paramName { get; set; }
    }

    public enum CriteriaCombine { AND, OR }
    public class CriteriaSet
    {
        public CriteriaCombine combine { get; set; } = CriteriaCombine.AND;
        public List<Criteria> criteria { get; set; } = new List<Criteria>();

        public CriteriaSet() { }

        public CriteriaSet(Criteria criteria)
        {
            AddCriteria(criteria);
        }

        public static List<CriteriaSet> GenWhere(Criteria criteria)
        {
            return new List<CriteriaSet>{ new CriteriaSet(criteria) };
        }

        public static IEnumerable<CriteriaSet> GenWhere(IEnumerable<Criteria> criteria)
        {
            CriteriaSet set = new CriteriaSet();
            foreach (var c in criteria)
                set.AddCriteria(c);
            return new[] { new CriteriaSet() };
        }

        public void AddCriteria(Criteria criteria)
        {
            this.criteria.Add(criteria);
        }
    }

    public class Order // ORDER BY
    {
        public string field { get; set; }
        public bool descending { get; set; }
    }

    public class QueryGetRequest
    {
        public string from { get; set; } // FROM
        public List<CriteriaSet> where { get; set; }
        public List<Order> orderBy { get; set; }
        public int limit { get; set; }

        public Dictionary<string, object> cmdParams { get; set; }

        public QueryGetRequest AddParam(string name, object value)
        {
            if (cmdParams == null)
                cmdParams = new Dictionary<string, object>();
            cmdParams.Add(name, value);
            return this;
        }

        public QueryGetRequest AddOrder(string name, bool descending)
        {
            if (orderBy == null)
                orderBy = new List<Order>();
            orderBy.Add(new Order() { field = name, descending = descending });
            return this;
        }
    }

    public class Select : QueryGetRequest
    {
        public List<string> select { get; set; }
    }

    public class GetResponse
    {
        public List<Dictionary<string, object>> metadata { get; set; }
    }

    public class Delete
    {
        public string table { get; set; }
        public List<object> values { get; set; } = new List<object>();

        public Delete() { }

        public Delete(string table, object value)
        {
            this.table = table;
            values.Add(value);
        }

        public Delete(string table, IEnumerable<object> values)
        {
            this.table = table;
            this.values.AddRange(values);
        }

        public Delete AddValue(object value)
        {
            values.Add(value);
            return this;
        }
    }

    public class Schema
    {
        public string table { get; set; } // optional to get the full schema
    }

    public class SchemaResponse
    {
        // table name => column names, kept in order
        public ListDictionary<string, List<string>> tables { get; set; }
    }
}
