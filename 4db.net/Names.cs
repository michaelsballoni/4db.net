﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;

namespace fourdb
{
    public class NameObj
    {
        public int id;
        public int tableId;
        public string name;
        public bool isNumeric;
    }

    /// <summary>
    /// Implementation class for the columns in the virtual schema
    /// </summary>
    public static class Names
    {
        internal static string[] CreateSql
        {
            get
            {
                return new[]
                {
                    "CREATE TABLE names\n(\n" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE,\n" +
                    "tableid INTEGER NOT NULL,\n" +
                    "name TEXT NOT NULL,\n" +
                    "isNumeric BOOLEAN NOT NULL,\n" +
                    "FOREIGN KEY(tableid) REFERENCES tables(id)\n" +
                    ")",
                    "CREATE UNIQUE INDEX idx_names_name_tableid ON names (name, tableid)"
                };
            }
        }
        
        /// <summary>
        /// Remove all names rows from the database
        /// </summary>
        /// <param name="ctxt">Database connection</param>
        public static void Reset(Context ctxt)
        {
            sm_cache.Clear();
            sm_cacheBack.Clear();
            sm_isNumericCache.Clear();

            ctxt.Db.ExecuteSql("DELETE FROM names");
        }

        /// <summary>
        /// Given a column name and value, get back the row ID
        /// </summary>
        /// <param name="ctxt">Database connection</param>
        /// <param name="tableId">What table does this row go into?</param>
        /// <param name="name">What is the column name?</param>
        /// <param name="isNumeric">Is this column numeric or string?</param>
        /// <param name="noCreate">Should the command fail if no existing table matches?</param>
        /// <param name="noException">Should the command fail return -1 if name not found?</param>
        /// <returns>Database row ID</returns>
        public static async Task<int> GetIdAsync(Context ctxt, int tableId, string name, bool isNumeric = false, bool noCreate = false, bool noException = false)
        {
            int id;
            string cacheKey = $"{tableId}_{name}";
            if (sm_cache.TryGetValue(cacheKey, out id))
                return id;

            if (!Utils.IsWord(name))
                throw new FourDbException($"Names.GetId name is not valid: {name}");

            if (Utils.IsNameReserved(name))
                throw new FourDbException($"Names.GetId name is reserved: {name}");

            Dictionary<string, object> cmdParams = new Dictionary<string, object>();
            cmdParams.Add("@tableId", tableId);
            cmdParams.Add("@name", name);
            string selectSql =
                "SELECT id FROM names WHERE tableid = @tableId AND name = @name";
            object idObj = await ctxt.Db.ExecuteScalarAsync(selectSql, cmdParams).ConfigureAwait(false);
            id = Utils.ConvertDbInt32(idObj);
            if (id >= 0)
            {
                sm_cache[cacheKey] = id;
                return id;
            }

            if (noCreate)
            {
                if (noException)
                    return -1;
                else
                    throw new FourDbException($"Names.GetId cannot find name: {name}");
            }

	        cmdParams.Add("@isNumeric", isNumeric);
	        string insertSql =
	            "INSERT INTO names (tableid, name, isNumeric) VALUES (@tableId, @name, @isNumeric)";
	        id = (int)await ctxt.Db.ExecuteInsertAsync(insertSql, cmdParams).ConfigureAwait(false);
	        
            sm_cache[cacheKey] = id;
	        return id;
       }

        /// <summary>
        /// Given a name ID, get info about the name
        /// </summary>
        /// <param name="ctxt">Database connection</param>
        /// <param name="id">Database row ID</param>
        /// <returns>Info about the name</returns>
        public static async Task<NameObj> GetNameAsync(Context ctxt, int id)
        {
            if (id < 0)
                return null;

            NameObj obj;
            if (sm_cacheBack.TryGetValue(id, out obj))
                return obj;

            string sql = $"SELECT tableid, name, isNumeric FROM names WHERE id = {id}";
            using (var reader = await ctxt.Db.ExecuteReaderAsync(sql).ConfigureAwait(false))
            {
                if (!await reader.ReadAsync().ConfigureAwait(false))
                    throw new FourDbException($"Names.GetName fails to find record: {id}");

                obj = new NameObj()
                {
                    id = id,
                    tableId = reader.GetInt32(0),
                    name = reader.GetString(1),
                    isNumeric = reader.GetBoolean(2)
                };
                sm_cacheBack[id] = obj;
                return obj;
            }
        }

        /// <summary>
        /// Given a name ID, see if it's numeric
        /// </summary>
        /// <param name="ctxt">Database connection</param>
        /// <param name="id">Name database row ID</param>
        /// <returns>true if the name is numeric</returns>
        public static async Task<bool> GetNameIsNumericAsync(Context ctxt, int id)
        {
            if (id < 0)
                return false;

            bool isNumeric;
            if (sm_isNumericCache.TryGetValue(id, out isNumeric))
                return isNumeric;

            string sql = $"SELECT isNumeric FROM names WHERE id = {id}";
            long numericNum = Utils.ConvertDbInt64(await ctxt.Db.ExecuteScalarAsync(sql).ConfigureAwait(false));
            isNumeric = numericNum != 0;
            sm_isNumericCache[id] = isNumeric;
            return isNumeric;
        }

        internal static void ClearCaches()
        {
            sm_cache.Clear();
            sm_cacheBack.Clear();
            sm_isNumericCache.Clear();
        }

        private static ConcurrentDictionary<string, int> sm_cache = new ConcurrentDictionary<string, int>();
        private static ConcurrentDictionary<int, NameObj> sm_cacheBack = new ConcurrentDictionary<int, NameObj>();
        private static ConcurrentDictionary<int, bool> sm_isNumericCache = new ConcurrentDictionary<int, bool>();
    }
}
