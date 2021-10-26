using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace fourdb
{
    /// <summary>
    /// metastrings implemetation for tracking the row data in metastrings tables
    /// Values can either be strings or floating-point numbers
    /// This class takes the form of Names and Tables, where you take what you know, in this case a value,
    /// and you get back what you want, the ID of the value's row in the bvalues MySQL table,
    /// or you have a MySQL table row ID, and you want to the value back out
    /// </summary>
    public static class Values
    {
        internal static string[] CreateSql
        {
            get
            {
                return new[]
                {
                    "CREATE TABLE bvalues\n(\n" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE,\n" +
                    "isNumeric BOOLEAN NOT NULL,\n" +
                    "numberValue NUMBER NOT NULL,\n" +
                    "stringValue TEXT\n" +
                    ")",

                    "CREATE UNIQUE INDEX idx_bvalues_unique ON bvalues (stringValue, numberValue, isNumeric)",

                    "CREATE INDEX idx_bvalues_prefix ON bvalues (stringValue, isNumeric, id)",
                    "CREATE INDEX idx_bvalues_number ON bvalues (numberValue, isNumeric, id)",

                    "CREATE VIRTUAL TABLE bvaluetext USING fts5 (valueid, stringSearchValue)"
                };
            }
        }

        /// <summary>
        /// Return this to the factory original
        /// </summary>
        /// <param name="ctxt">Object for interacting with the database</param>
        public static void Reset(Context ctxt)
        {
            ctxt.Db.ExecuteSql("DELETE FROM bvalues");
            ctxt.Db.ExecuteSql("DELETE FROM bvaluetext");
        }

        /// <summary>
        /// Given a value, get the row ID in the MySQL bvalues table.
        /// Note that there's caching 
        /// </summary>
        /// <param name="ctxt">Object for interacting with the database</param>
        /// <param name="value">
        /// Could be anything, but it's a either a string, or it better be convertible to a double
        /// Note that row IDs are cached, but only for strings or for numbers that are essentially integral
        /// </param>
        /// <returns>row ID in MySQL table</returns>
        public static async Task<long> GetIdAsync(Context ctxt, object value)
        {
            var totalTimer = ScopeTiming.StartTiming();
            try
            {
                if (value == null)
                    return -1;

                long id = -1;
                Exception lastExp = null;
                for (int tryCount = 1; tryCount <= 3; ++tryCount)
                {
                    try
                    {
                        id = await GetIdSelectAsync(ctxt, value).ConfigureAwait(false);
                        if (id >= 0)
                            break;

                        id = await GetIdInsertAsync(ctxt, value).ConfigureAwait(false);
                        break;
                    }
                    catch (Exception exp)
                    {
                        lastExp = exp;
                    }
                }

                if (id >= 0)
                    return id;

                throw new MetaStringsException("Values.GetId failed after a few retries", lastExp);
            }
            finally
            {
                ScopeTiming.RecordScope("Values.GetId", totalTimer);
            }
        }

        private static async Task<long> GetIdSelectAsync(Context ctxt, object value)
        {
            var localTimer = ScopeTiming.StartTiming();

            if (value is string)
            {
                string strValue = (string)value;
                Dictionary<string, object> cmdParams = new Dictionary<string, object>();
                cmdParams.Add("@stringValue", strValue);
                string selectSql =
                    "SELECT id FROM bvalues WHERE isNumeric = 0 AND stringValue = @stringValue";
                long id = Utils.ConvertDbInt64(await ctxt.Db.ExecuteScalarAsync(selectSql, cmdParams).ConfigureAwait(false));
                ScopeTiming.RecordScope($"Values.GetId.SELECT(string)", localTimer);
                return id;
            }
            else
            {
                double numberValue = Convert.ToDouble(value);
                Dictionary<string, object> cmdParams = new Dictionary<string, object>();
                cmdParams.Add("@numberValue", numberValue);
                string selectSql =
                    "SELECT id FROM bvalues WHERE isNumeric = 1 AND numberValue = @numberValue";
                long id = Utils.ConvertDbInt64(await ctxt.Db.ExecuteScalarAsync(selectSql, cmdParams).ConfigureAwait(false));
                ScopeTiming.RecordScope("Values.GetId.SELECT(number)", localTimer);
                return id;
            }
        }

        private static async Task<long> GetIdInsertAsync(Context ctxt, object value)
        {
            if (value is string)
            {
                string strValue = (string)value;
                Dictionary<string, object> cmdParams = new Dictionary<string, object>();
                cmdParams.Add("@stringValue", strValue);
                string insertSql =
                    "INSERT INTO bvalues (isNumeric, numberValue, stringValue) VALUES (0, 0.0, @stringValue)";
                long id = await ctxt.Db.ExecuteInsertAsync(insertSql, cmdParams).ConfigureAwait(false);
                
                string textInsertSearchSql =
                    $"INSERT INTO bvaluetext (valueid, stringSearchValue) VALUES ({id}, @stringValue)";
                await ctxt.Db.ExecuteInsertAsync(textInsertSearchSql, cmdParams, returnNewId: false);
                
                return id;
            }
            else
            {
                double numberValue = Convert.ToDouble(value);
                Dictionary<string, object> cmdParams = new Dictionary<string, object>();
                cmdParams.Add("@numberValue", numberValue);
                string insertSql =
                    "INSERT INTO bvalues (isNumeric, numberValue, stringValue) VALUES (1, @numberValue, '')";
                long id = await ctxt.Db.ExecuteInsertAsync(insertSql, cmdParams).ConfigureAwait(false);
                return id;
            }
        }

        /// <summary>
        /// Given a row ID in the MySQL bvalues table, get out the value
        /// </summary>
        /// <param name="ctxt">Object for interacting with the database</param>
        /// <param name="id">row ID in the MySQL bvalues table</param>
        /// <returns></returns>
        public static async Task<object> GetValueAsync(Context ctxt, long id)
        {
            var totalTimer = ScopeTiming.StartTiming();
            try
            {
                string sql = $"SELECT isNumeric, numberValue, stringValue FROM bvalues WHERE id = {id}";
                using (var reader = await ctxt.Db.ExecuteReaderAsync(sql).ConfigureAwait(false))
                { 
                    if (!await reader.ReadAsync().ConfigureAwait(false))
                        throw new MetaStringsException("Values.GetValue fails to find record with ID = " + id);
                    object toReturn;
                    bool isNumeric = reader.GetBoolean(0);
                    if (isNumeric)
                        toReturn = reader.GetDouble(1);
                    else
                        toReturn = reader.GetString(2);
                    return toReturn;
                }
            }
            finally
            {
                ScopeTiming.RecordScope("Values.GetValue", totalTimer);
            }
        }
    }
}
