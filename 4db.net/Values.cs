using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace fourdb
{
    /// <summary>
    /// Implemetation for tracking the row data in virtual tables
    /// Values can either be strings or floating-point numbers
    /// This class takes the form of Names and Tables, where you take what you know, in this case a value,
    /// and you get back what you want, the ID of the value's row in the database table,
    /// or you have a row ID, and you want to the value back out
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
        /// Given a value, get the row ID in the database
        /// </summary>
        /// <param name="ctxt">Datbase connection</param>
        /// <param name="value">
        /// Could be anything, but it's a either a string, or it better be convertible to a double
        /// </param>
        /// <returns>row ID in database</returns>
        public static async Task<long> GetIdAsync(Context ctxt, object value)
        {
            if (value == null)
                return -1;

            long id = -1;
            id = await GetIdSelectAsync(ctxt, value).ConfigureAwait(false);
            if (id >= 0)
                return id;

            id = await GetIdInsertAsync(ctxt, value).ConfigureAwait(false);
            return id;
        }

        private static async Task<long> GetIdSelectAsync(Context ctxt, object value)
        {
            if (value is string)
            {
                string strValue = (string)value;
                Dictionary<string, object> cmdParams = new Dictionary<string, object>();
                cmdParams.Add("@stringValue", strValue);
                string selectSql =
                    "SELECT id FROM bvalues WHERE isNumeric = 0 AND stringValue = @stringValue";
                long id = Utils.ConvertDbInt64(await ctxt.Db.ExecuteScalarAsync(selectSql, cmdParams).ConfigureAwait(false));
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
        /// Given a row ID in the database, get out the value
        /// </summary>
        /// <param name="ctxt">Database connection</param>
        /// <param name="id">row ID in the database</param>
        /// <returns></returns>
        public static async Task<object> GetValueAsync(Context ctxt, long id)
        {
            string sql = $"SELECT isNumeric, numberValue, stringValue FROM bvalues WHERE id = {id}";
            using (var reader = await ctxt.Db.ExecuteReaderAsync(sql).ConfigureAwait(false))
            { 
                if (!await reader.ReadAsync().ConfigureAwait(false))
                    throw new FourDbException("Values.GetValue fails to find record with ID = " + id);
                object toReturn;
                bool isNumeric = reader.GetBoolean(0);
                if (isNumeric)
                    toReturn = reader.GetDouble(1);
                else
                    toReturn = reader.GetString(2);
                return toReturn;
            }
        }
    }
}
