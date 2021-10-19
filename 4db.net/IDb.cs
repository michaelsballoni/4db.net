using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Data.Common;

namespace fourdb
{
    /// <summary>
    /// Generic database type
    /// Useful when working with MySQL vs. SQLite
    /// </summary>
    public interface IDb : IDisposable
    {
        MsTrans BeginTrans();
        void Commit();
        int TransCount { get; set; }
        void FreeTrans();

        string InsertIgnore { get; }
        string UtcTimestampFunction { get; }

        int ExecuteSql(string sql, Dictionary<string, object> cmdParams = null);
        Task<int> ExecuteSqlAsync(string sql, Dictionary<string, object> cmdParams = null);
        Task<long> ExecuteInsertAsync(string sql, Dictionary<string, object> cmdParams = null, bool returnNewId = true);
        Task<object> ExecuteScalarAsync(string sql, Dictionary<string, object> cmdParams = null);
        Task<DbDataReader> ExecuteReaderAsync(string sql, Dictionary<string, object> cmdParams = null);
    }
}
