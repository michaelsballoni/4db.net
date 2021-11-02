using System;

namespace fourdb
{
    public class FourDbException : Exception
    {
        public FourDbException(string msg) : base(msg) { }
        public FourDbException(string msg, Exception innerExp) : base(msg, innerExp) { }
    }

    public class SqlException : FourDbException
    {
        public SqlException(string msg, string sql) : base(msg) { Sql = sql; }

        public string Sql { get; private set; }
    }
}
