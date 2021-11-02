using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace fourdb
{
    public class TableObj
    {
        public int id;
        public string name;
        public bool isNumeric;
    }

    /// <summary>
    /// Implementation class for the tables in the virtual schema
    /// </summary>
    public static class Tables
    {
        internal static string[] CreateSql
        {
            get
            {
                return new[]
                {
                    "CREATE TABLE tables\n(\n" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE,\n" +
                    "name TEXT NOT NULL UNIQUE,\n" +
                    "isNumeric BOOLEAN NOT NULL\n" +
                    ")"
                };
            }
        }

        /// <summary>
        /// Remove all tables from the database
        /// </summary>
        /// <param name="ctxt">Database connection</param>
        public static void Reset(Context ctxt)
        {
            sm_cache.Clear();
            sm_cacheBack.Clear();

            ctxt.Db.ExecuteSql("DELETE FROM tables");
        }

        /// <summary>
        /// Given a table name, get the row ID for the table
        /// </summary>
        /// <param name="ctxt">Database connection</param>
        /// <param name="name">Table name</param>
        /// <param name="isNumeric">Is the table's primary key numeric or string</param>
        /// <param name="noCreate">Should an exception be thrown if no table found</param>
        /// <param name="noException">Should -1 be returned instead of throwing an exception if the table is not found</param>
        /// <returns>Database row ID for the table</returns>
        public static async Task<int> GetIdAsync(Context ctxt, string name, bool isNumeric = false, bool noCreate = false, bool noException = false)
        {
            int id;
            if (sm_cache.TryGetValue(name, out id))
                return id;

            if (!Utils.IsWord(name))
                throw new FourDbException($"Types.GetId name is not valid: {name}");

            if (Utils.IsNameReserved(name))
                throw new FourDbException($"Types.GetId name is reserved: {name}");

            Exception lastExp = null;
            bool isExpFinal = false;
            for (int tryCount = 1; tryCount <= 4; ++tryCount)
            {
                try
                {
                    Dictionary<string, object> cmdParams = new Dictionary<string, object>();
                    cmdParams.Add("@name", name);
                    string selectSql = "SELECT id FROM tables WHERE name = @name";
                    object idObj = await ctxt.Db.ExecuteScalarAsync(selectSql, cmdParams).ConfigureAwait(false);
                    id = Utils.ConvertDbInt32(idObj);
                    if (id >= 0)
                    {
                        sm_cache[name] = id;
                        return id;
                    }

                    if (noCreate)
                    {
                        if (noException)
                            return -1;

                        isExpFinal = true;
                        throw new FourDbException($"Tables.GetId cannot create new table: {name}", lastExp);
                    }

                    cmdParams.Add("@isNumeric", isNumeric);
                    string insertSql = "INSERT INTO tables (name, isNumeric) VALUES (@name, @isNumeric)";
                    id = (int)await ctxt.Db.ExecuteInsertAsync(insertSql, cmdParams).ConfigureAwait(false);
                    sm_cache[name] = id;
                    return id;
                }
                catch (Exception exp)
                {
                    if (isExpFinal)
                        throw exp;

                    lastExp = exp;
                }
            }

            throw new FourDbException("Tables.GetId fails after a few tries", lastExp);
        }

        /// <summary>
        /// Get info about the table found by looking up the row ID
        /// </summary>
        /// <param name="ctxt">Database connection</param>
        /// <param name="id">Table database row ID</param>
        /// <returns></returns>
        public static async Task<TableObj> GetTableAsync(Context ctxt, int id)
        {
            if (id < 0)
                return null;

            TableObj obj;
            if (sm_cacheBack.TryGetValue(id, out obj))
                return obj;

            string sql = $"SELECT name, isNumeric FROM tables WHERE id = {id}";
            using (var reader = await ctxt.Db.ExecuteReaderAsync(sql).ConfigureAwait(false))
            {
                if (!await reader.ReadAsync().ConfigureAwait(false))
                    throw new FourDbException($"Tables.GetTable fails to find record: {id}");

                obj =
                    new TableObj()
                    {
                        id = id,
                        name = reader.GetString(0),
                        isNumeric = reader.GetBoolean(1)
                    };
                sm_cacheBack[id] = obj;
                return obj;
            }
        }

        internal static void ClearCaches()
        {
            sm_cache.Clear();
            sm_cacheBack.Clear();
        }

        private static ConcurrentDictionary<string, int> sm_cache = new ConcurrentDictionary<string, int>();
        private static ConcurrentDictionary<int, TableObj> sm_cacheBack = new ConcurrentDictionary<int, TableObj>();
    }
}
