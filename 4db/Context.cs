using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SQLite;
using System.IO;

namespace fourdb
{
    /// <summary>
    /// Context manages the database connection
    /// and provides useful query helper functions
    /// </summary>
    public class Context : IDisposable
    {
        /// <summary>
        /// Create a context for a database connection
        /// </summary>
        /// <param name="dbConnStr">Database connection string...we're out of the config business</param>
        public Context(string dbConnStr)
        {
            string dbFilePath = DbConnStrToFilePath(dbConnStr);
            string actualDbConnStr = "Data Source=" + dbFilePath;

            lock (sm_dbBuildLock)
            {
                if (!(File.Exists(dbFilePath) && new FileInfo(dbFilePath).Length > 0))
                {
                    SQLiteConnection.CreateFile(dbFilePath);

                    using (var db = new SqlLiteDb(actualDbConnStr))
                    {
                        RunSql(db, Tables.CreateSql);
                        RunSql(db, Names.CreateSql);
                        RunSql(db, Values.CreateSql);
                        RunSql(db, Items.CreateSql);
                    }
                }

                using (var db = new SqlLiteDb(actualDbConnStr))
                    RunSql(db, new[] { "PRAGMA journal_mode = WAL", "PRAGMA synchronous = NORMAL" });
            }

            Db = new SqlLiteDb(actualDbConnStr);
        }

        public void Dispose()
        {
            if (Db != null)
            {
                Db.Dispose();
                Db = null;
            }

            if (m_postItemOps != null && m_postItemOps.Count > 0)
                throw new MetaStringsException("Post ops remain; call ProcessPostOpsAsync before disposing the metastrings context");
        }

        /// <summary>
        /// The database connection
        /// </summary>
        public IDb Db { get; private set; }

        /// <summary>
        /// Transactions are supported, 
        /// but should not be used around any code affecting data 
        /// in the Table, Name, Value, etc. metastrings database
        /// as rollbacks would break the global in-memory caching
        /// </summary>
        /// <returns>Transaction object</returns>
        public MsTrans BeginTrans()
        {
            return Db.BeginTrans();
        }

        /// <summary>
        /// Query helper function to get a reader for a query
        /// </summary>
        /// <param name="select">Query to execute</param>
        /// <returns>Reader to get results from</returns>
        public async Task<DbDataReader> ExecSelectAsync(Select select)
        {
            var cmdParams = select.cmdParams;
            var sql = await Sql.GenerateSqlAsync(this, select).ConfigureAwait(false);
            return await Db.ExecuteReaderAsync(sql, cmdParams).ConfigureAwait(false);
        }

        /// <summary>
        /// Query helper function to get a single value for a query
        /// </summary>
        /// <param name="select">Query to execute</param>
        /// <returns>The single query result value</returns>
        public async Task<object> ExecScalarAsync(Select select)
        {
            var cmdParams = select.cmdParams;
            var sql = await Sql.GenerateSqlAsync(this, select).ConfigureAwait(false);
            return await Db.ExecuteScalarAsync(sql, cmdParams).ConfigureAwait(false);
        }

        /// <summary>
        /// Query helper to get a single 64-bit integer query result
        /// </summary>
        /// <param name="select">Query to execute</param>
        /// <returns>64-bit result value, or -1 if processing fails</returns>
        public async Task<long> ExecScalar64Async(Select select)
        {
            object result = await ExecScalarAsync(select).ConfigureAwait(false);
            long val = Utils.ConvertDbInt64(result);
            return val;
        }

        /// <summary>
        /// Query helper to get a list of results from a single-column query
        /// </summary>
        /// <param name="select">Query to execute</param>
        /// <returns>List of results of type T</returns>
        public async Task<List<T>> ExecListAsync<T>(Select select)
        {
            var values = new List<T>();
            using (var reader = await ExecSelectAsync(select).ConfigureAwait(false))
            {
                while (await reader.ReadAsync().ConfigureAwait(false))
                    values.Add((T)reader.GetValue(0));
            }
            return values;
        }

        /// <summary>
        /// Query helper to get a dictionary of results from a single-column query
        /// </summary>
        /// <param name="select">Query to execute</param>
        /// <returns>ListDictionary of results of type K, V</returns>
        public async Task<ListDictionary<K, V>> ExecDictAsync<K, V>(Select select)
        {
            var values = new ListDictionary<K, V>();
            using (var reader = await ExecSelectAsync(select).ConfigureAwait(false))
            {
                while (await reader.ReadAsync().ConfigureAwait(false))
                    values.Add((K)reader.GetValue(0), (V)reader.GetValue(1));
            }
            return values;
        }

        /// <summary>
        /// Get the items table row ID for a given table and key
        /// </summary>
        /// <param name="tableName">Table to look in</param>
        /// <param name="key">Key of the item in the table</param>
        /// <returns>Row ID, or -1 if not found</returns>
        public async Task<long> GetRowIdAsync(string tableName, object key)
        {
            Utils.ValidateTableName(tableName, "GetRowId");
            Select select = Sql.Parse($"SELECT id FROM {tableName} WHERE value = @value");
            select.AddParam("@value", key);
            long id = await ExecScalar64Async(select).ConfigureAwait(false);
            return id;
        }

        /// <summary>
        /// Get the object value from the given table and items table ID
        /// </summary>
        /// <param name="table">Table to look in</param>
        /// <param name="id">Row ID to look for</param>
        /// <returns>object value if found, null otherwise</returns>
        public async Task<object> GetRowValueAsync(string table, long id)
        {
            Utils.ValidateTableName(table, "GetRowValueAsync");
            Select select = Sql.Parse($"SELECT value FROM {table} WHERE id = @id");
            select.AddParam("@id", id);
            object val = await ExecScalarAsync(select).ConfigureAwait(false);
            return val;
        }

        /// <summary>
        /// This is the main UPSERT method to populate the database.
        /// </summary>
        /// <param name="define">Info about metadata to apply to the key</param>
        public async Task DefineAsync(Define define)
        {
            var totalTimer = ScopeTiming.StartTiming();
            try
            {
                var localTimer = ScopeTiming.StartTiming();

                bool isKeyNumeric = !(define.key is string);
                int tableId = await Tables.GetIdAsync(this, define.table, isKeyNumeric).ConfigureAwait(false);
                long valueId = await Values.GetIdAsync(this, define.key).ConfigureAwait(false);
                long itemId = await Items.GetIdAsync(this, tableId, valueId).ConfigureAwait(false);
                ScopeTiming.RecordScope("Define.Setup", localTimer);

                if (define.metadata != null)
                {
                    // name => nameid
                    var nameValueIds = new Dictionary<int, long>();
                    foreach (var kvp in define.metadata)
                    {
                        bool isMetadataNumeric = !(kvp.Value is string);
                        int nameId = await Names.GetIdAsync(this, tableId, kvp.Key, isMetadataNumeric).ConfigureAwait(false);
                        if (kvp.Value == null) // erase value
                        {
                            nameValueIds[nameId] = -1;
                            continue;
                        }
                        bool isNameNumeric = await Names.GetNameIsNumericAsync(this, nameId).ConfigureAwait(false);
                        bool isValueNumeric = !(kvp.Value is string);
                        if (isValueNumeric != isNameNumeric)
                        {
                            throw
                                new MetaStringsException
                                (
                                    $"Data numeric does not match name: {kvp.Key}" +
                                    $"\n - value is numeric: {isValueNumeric} - {kvp.Value}" +
                                    $"\n - name is numeric: {isNameNumeric}"
                                );
                        }
                        nameValueIds[nameId] =
                            await Values.GetIdAsync(this, kvp.Value).ConfigureAwait(false);
                    }
                    ScopeTiming.RecordScope("Define.NameIds", localTimer);

                    Items.SetItemData(this, itemId, nameValueIds);
                    ScopeTiming.RecordScope("Define.ItemsCommit", localTimer);
                }

                await this.ProcessPostOpsAsync().ConfigureAwait(false);
                ScopeTiming.RecordScope("Define.PostOps", localTimer);
            }
#if !DEBUG
            catch
            {
                this.ClearPostOps();
                throw;
            }
#endif
            finally
            {
                ScopeTiming.RecordScope("Define", totalTimer);
            }
        }

        /// <summary>
        /// Generate SQL query given a Select object
        /// This is where the metastrings -> SQL magic happens
        /// </summary>
        /// <param name="query">NoSQL query object</param>
        /// <returns>SQL query</returns>
        public async Task<string> GenerateSqlAsync(Select query)
        {
            var totalTimer = ScopeTiming.StartTiming();
            try
            {
                string sql = await Sql.GenerateSqlAsync(this, query).ConfigureAwait(false);
                return sql;
            }
            finally
            {
                ScopeTiming.RecordScope("Cmd.GenerateSql", totalTimer);
            }
        }

        /// <summary>
        /// Get the metadata for a set of items
        /// </summary>
        /// <param name="request">List of values to get metadata for</param>
        /// <returns>Metadata for the items</returns>
        public async Task<GetResponse> GetAsync(GetRequest request)
        {
            var totalTimer = ScopeTiming.StartTiming();
            try
            {
                var responses = new List<Dictionary<string, object>>(request.values.Count);

                int tableId = await Tables.GetIdAsync(this, request.table, noCreate: true).ConfigureAwait(false);
                foreach (var value in request.values)
                {
                    long valueId = await Values.GetIdAsync(this, value).ConfigureAwait(false);

                    long itemId = await Items.GetIdAsync(this, tableId, valueId, noCreate: true).ConfigureAwait(false);
                    if (itemId < 0)
                    {
                        responses.Add(null);
                        continue;
                    }

                    var metaIds = await Items.GetItemDataAsync(this, itemId).ConfigureAwait(false);
                    var metaStrings = await NameValues.GetMetadataValuesAsync(this, metaIds).ConfigureAwait(false);

                    responses.Add(metaStrings);
                }

                GetResponse response = new GetResponse() { metadata = responses };
                return response;
            }
            finally
            {
                ScopeTiming.RecordScope("Cmd.Get", totalTimer);
            }
        }

        /// <summary>
        /// Query for the metadata for a set of items.
        /// </summary>
        /// <param name="request">NoSQL query for items to get</param>
        /// <returns>Metadata of found items</returns>
        public async Task<GetResponse> QueryGetAsync(QueryGetRequest request)
        {
            var totalTimer = ScopeTiming.StartTiming();
            try
            {
                var itemValues = new Dictionary<long, object>();
                {
                    Select select = new Select();
                    select.select = new List<string> { "id", "value" };
                    select.from = request.from;
                    select.where = request.where;
                    select.orderBy = request.orderBy;
                    select.limit = request.limit;
                    select.cmdParams = request.cmdParams;
                    using (var reader = await this.ExecSelectAsync(select).ConfigureAwait(false))
                    {
                        while (await reader.ReadAsync().ConfigureAwait(false))
                            itemValues.Add(reader.GetInt64(0), reader.GetValue(1));
                    }
                }

                var responses = new List<Dictionary<string, object>>(itemValues.Count);
                foreach (var itemId in itemValues.Keys)
                {
                    var metaIds = await Items.GetItemDataAsync(this, itemId).ConfigureAwait(false);
                    var metaStrings = await NameValues.GetMetadataValuesAsync(this, metaIds).ConfigureAwait(false);

                    metaStrings["id"] = (double)itemId;
                    metaStrings["value"] = itemValues[itemId];

                    responses.Add(metaStrings);
                }

                GetResponse response = new GetResponse() { metadata = responses };
                return response;
            }
            finally
            {
                ScopeTiming.RecordScope("Cmd.QueryGet", totalTimer);
            }
        }

        /// <summary>
        /// Delete a single item from a table.
        /// </summary>
        /// <param name="table">Table to delete from</param>
        /// <param name="value">Value of object to delete</param>
        public async Task DeleteAsync(string table, object value)
        {
            await DeleteAsync(new Delete(table, value)).ConfigureAwait(false);
        }

        /// <summary>
        /// Delete multiple items from a table.
        /// </summary>
        /// <param name="table">Table to delete from</param>
        /// <param name="values">Values of objects to delete</param>
        public async Task DeleteAsync(string table, IEnumerable<object> values)
        {
            await DeleteAsync(new Delete(table, values)).ConfigureAwait(false);
        }

        /// <summary>
        /// Process a delete request.
        /// </summary>
        /// <param name="toDelete">Delete reqeuest</param>
        public async Task DeleteAsync(Delete toDelete)
        {
            var totalTimer = ScopeTiming.StartTiming();
            try
            {
                int tableId = await Tables.GetIdAsync(this, toDelete.table, noCreate: true, noException: true).ConfigureAwait(false);
                if (tableId < 0)
                    return;

                foreach (var val in toDelete.values)
                {
                    long valueId = await Values.GetIdAsync(this, val).ConfigureAwait(false);
                    string sql = $"DELETE FROM items WHERE valueid = {valueId} AND tableid = {tableId}";
                    this.AddPostOp(sql);
                }

                await this.ProcessPostOpsAsync().ConfigureAwait(false);
            }
            finally
            {
                ScopeTiming.RecordScope("Cmd.Delete", totalTimer);
            }
        }

        /// <summary>
        /// Drop a table from the database schema
        /// </summary>
        /// <param name="table">Name of table to drop</param>
        public async Task DropAsync(string table)
        {
            var totalTimer = ScopeTiming.StartTiming();
            try
            {
                NameValues.ClearCaches();

                int tableId = await Tables.GetIdAsync(this, table, noCreate: true, noException: true).ConfigureAwait(false);
                if (tableId < 0)
                    return;

                await this.Db.ExecuteSqlAsync($"DELETE FROM itemnamevalues WHERE nameid IN (SELECT id FROM names WHERE tableid = {tableId})").ConfigureAwait(false);
                await this.Db.ExecuteSqlAsync($"DELETE FROM names WHERE tableid = {tableId}").ConfigureAwait(false);
                await this.Db.ExecuteSqlAsync($"DELETE FROM items WHERE tableid = {tableId}").ConfigureAwait(false);
                await this.Db.ExecuteSqlAsync($"DELETE FROM tables WHERE id = {tableId}").ConfigureAwait(false);

                NameValues.ClearCaches();
            }
            finally
            {
                ScopeTiming.RecordScope("Cmd.Drop", totalTimer);
            }
        }

        /// <summary>
        /// Reset the metastrings database
        /// Only used internally for testing, should not be used in a production environment
        /// </summary>
        /// <param name="reset">Reset request object</param>
        public void Reset(bool includeNameValues = false)
        {
            if (includeNameValues)
                NameValues.Reset(this);
            else
                Items.Reset(this);

            NameValues.ClearCaches();
        }

        /// <summary>
        /// Get the schema of a metastrings database
        /// </summary>
        /// <param name="table">Name of table to get the schema of</param>
        /// <returns>Schema object</returns>
        public async Task<SchemaResponse> GetSchemaAsync(string table)
        {
            string sql =
                "SELECT t.name AS tablename, n.name AS colname " +
                "FROM tables t JOIN names n ON n.tableid = t.id";

            string requestedTable = table;
            bool haveRequestedTableName = !string.IsNullOrWhiteSpace(requestedTable);
            if (haveRequestedTableName)
                sql += " WHERE t.name = @name";

            sql += " ORDER BY tablename, colname";

            Dictionary<string, object> cmdParams = new Dictionary<string, object>();
            if (haveRequestedTableName)
                cmdParams.Add("@name", requestedTable);

            var responseDict = new ListDictionary<string, List<string>>();
            using (var reader = await this.Db.ExecuteReaderAsync(sql, cmdParams).ConfigureAwait(false))
            {
                while (await reader.ReadAsync().ConfigureAwait(false))
                {
                    string curTable = reader.GetString(0);
                    string colname = reader.GetString(1);

                    if (!responseDict.ContainsKey(curTable))
                        responseDict.Add(curTable, new List<string>());

                    responseDict[curTable].Add(colname);
                }
            }

            SchemaResponse response = new SchemaResponse() { tables = responseDict };
            return response;
        }

        /// <summary>
        /// Explicitly create a table in the schema.
        /// This is usually unnecessary as tables are created as referred to by Define.
        /// </summary>
        /// <param name="name">Table name to create request</param>
        public async Task CreateTableAsync(string name, bool isNumeric)
        {
            await Tables.GetIdAsync(this, name, isNumeric).ConfigureAwait(false);
        }

        /// <summary>
        /// Process queries that piled up by Command's Define function
        /// This is the rare case where using a transaction is well-advised
        /// </summary>
        public async Task ProcessPostOpsAsync()
        {
            if (m_postItemOps == null || m_postItemOps.Count == 0)
                return;

            var totalTimer = ScopeTiming.StartTiming();
            try
            {
                using (var msTrans = BeginTrans())
                {
                    foreach (string sql in m_postItemOps)
                        await Db.ExecuteSqlAsync(sql).ConfigureAwait(false);
                    msTrans.Commit();
                }
            }
            finally
            {
                m_postItemOps.Clear();
                ScopeTiming.RecordScope("ProcessItemPostOps", totalTimer);
            }
        }

        internal void AddPostOp(string sql)
        {
            if (m_postItemOps == null)
                m_postItemOps = new List<string>();
            m_postItemOps.Add(sql);
        }
        private List<string> m_postItemOps;

        public static string DbConnStrToFilePath(string connStr)
        {
            string filePath = connStr;

            int equals = filePath.IndexOf('=');
            if (equals > 0)
                filePath = filePath.Substring(equals + 1);

            filePath = filePath.Replace("[UserRoaming]", Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData));
            filePath = filePath.Replace("[MyDocuments]", Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments));
    
            string directoryPath = Path.GetDirectoryName(filePath);
            if (directoryPath.Length > 0 && !Directory.Exists(directoryPath))
                Directory.CreateDirectory(directoryPath);
            
            return filePath;
        }

        private static void RunSql(IDb db, string[] sqlQueries)
        {
            foreach (string sql in sqlQueries)
                db.ExecuteSql(sql);
        }

        private static object sm_dbBuildLock = new object();
    }
}
