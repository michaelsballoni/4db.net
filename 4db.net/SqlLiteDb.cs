using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Data;
using System.Data.Common;

using System.Data.SQLite;

namespace fourdb
{
    /// <summary>
    /// Database interop class for SQLite
    /// First, there was MySQL...!?!?
    /// </summary>
    public class SqlLiteDb : IDb
    {
        public SqlLiteDb(string dbConnStr)
        {
            DbConn = new SQLiteConnection(dbConnStr);
            DbConn.Open();
            DbConn.EnableExtensions(true);
            DbConn.LoadExtension("SQLite.Interop.dll", "sqlite3_fts5_init");
        }

        public void Dispose()
        {
            if (DbTrans != null)
            {
                DbTrans.Dispose();
                DbTrans = null;
            }

            if (DbConn != null)
            {
                DbConn.Dispose();
                DbConn = null;
            }
        }

        public MsTrans BeginTrans()
        {
            if (DbTrans == null)
                DbTrans = DbConn.BeginTransaction();
            return new MsTrans(this);
        }

        public void Commit()
        {
            DbTrans.Commit();
        }

        public int TransCount { get; set; }

        public void FreeTrans()
        {
            if (DbTrans != null)
            {
                var trans = DbTrans;
                DbTrans = null;
                trans.Dispose();
            }
        }

        public string InsertIgnore => "INSERT OR IGNORE INTO";
        public string UtcTimestampFunction => "DATETIME('now')";

        public int ExecuteSql(string sql, Dictionary<string, object> cmdParams = null)
        {
            int rowsAffected;
            using (var cmd = PrepCmd(sql, cmdParams))
                rowsAffected = cmd.ExecuteNonQuery();
            return rowsAffected;
        }

        public async Task<int> ExecuteSqlAsync(string sql, Dictionary<string, object> cmdParams = null)
        {
            int rowsAffected;
            using (var cmd = PrepCmd(sql, cmdParams))
                rowsAffected = await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
            return rowsAffected;
        }

        public async Task<long> ExecuteInsertAsync(string sql, Dictionary<string, object> cmdParams = null, bool returnNewId = true)
        {
            using (var cmd = PrepCmd(sql, cmdParams))
                await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
            if (returnNewId)
            {
                using (var cmd = PrepCmd("SELECT last_insert_rowid()"))
                    return Utils.ConvertDbInt64(await cmd.ExecuteScalarAsync().ConfigureAwait(false));
            }
            else
                return -1;
        }

        public async Task<object> ExecuteScalarAsync(string sql, Dictionary<string, object> cmdParams = null)
        {
            using (var cmd = PrepCmd(sql, cmdParams))
                return await cmd.ExecuteScalarAsync().ConfigureAwait(false);
        }

        public async Task<DbDataReader> ExecuteReaderAsync(string sql, Dictionary<string, object> cmdParams = null)
        {
            using (var cmd = PrepCmd(sql, cmdParams))
                return await cmd.ExecuteReaderAsync().ConfigureAwait(false);
        }

        private SQLiteCommand PrepCmd(string sql, Dictionary<string, object> cmdParams = null)
        {
            var cmd = new SQLiteCommand(sql, DbConn, DbTrans);
            if (cmdParams != null)
            {
                foreach (var kvp in cmdParams)
                    cmd.Parameters.AddWithValue(kvp.Key, kvp.Value);
            }
            return cmd;
        }

        private SQLiteConnection DbConn;
        private SQLiteTransaction DbTrans;
    }
}
