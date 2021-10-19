using System;

namespace fourdb
{
    public class MetaStringsException : Exception
    {
        public MetaStringsException(string msg) : base(msg) { }
        public MetaStringsException(string msg, Exception innerExp) : base(msg, innerExp) { }
    }

    public class SqlException : MetaStringsException
    {
        public SqlException(string msg, string sql)  
            : base(msg) 
        {
            Sql = sql;
        }

        public string Sql { get; private set; }
    }
}
