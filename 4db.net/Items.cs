using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Text;

namespace fourdb
{
    /// <summary>
    /// Items are the rows in the virtual schema
    /// </summary>
    public static class Items
    {
        internal static string[] CreateSql
        {
            get
            {
                return new[]
                {
                    "CREATE TABLE items\n(\n" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE,\n" +
                    "tableid INTEGER NOT NULL,\n" +
                    "valueid INTEGER NOT NULL,\n" +
                    "created TIMESTAMP NOT NULL,\n" +
                    "lastmodified TIMESTAMP NOT NULL,\n" +
                    "FOREIGN KEY(tableid) REFERENCES tables(id),\n" +
                    "FOREIGN KEY(valueid) REFERENCES bvalues(id)\n" +
                    ")",

                    "CREATE UNIQUE INDEX idx_items_valueid_tableid ON items (valueid, tableid)",
                    "CREATE INDEX idx_items_created ON items (created)",
                    "CREATE INDEX idx_items_lastmodified ON items (lastmodified)",

                    "CREATE TABLE itemnamevalues\n(\n" +
                    "itemid INTEGER NOT NULL,\n" +
                    "nameid INTEGER NOT NULL,\n" +
                    "valueid INTEGER NOT NULL,\n" +
                    "PRIMARY KEY (itemid, nameid),\n" +
                    "FOREIGN KEY(itemid) REFERENCES items(id),\n" +
                    "FOREIGN KEY(nameid) REFERENCES names(id),\n" +
                    "FOREIGN KEY(valueid) REFERENCES bvalues(id)\n" +
                    ")",

                    "CREATE VIEW itemvalues AS\n" +
                    "SELECT\n" +
                    "inv.itemid AS itemid,\n" +
                    "inv.nameid AS nameid,\n" +
                    "v.id AS valueid,\n" +
                    "v.isNumeric AS isNumeric,\n" +
                    "v.numberValue AS numberValue,\n" +
                    "v.stringValue AS stringValue\n" +
                    "FROM itemnamevalues AS inv\n" +
                    "JOIN bvalues AS v ON v.id = inv.valueid"
                };
            }
        }

        /// <summary>
        /// Remove all rows from the items table
        /// </summary>
        public static void Reset(Context ctxt)
        {
            ctxt.Db.ExecuteSql("DELETE FROM items");
        }

        /// <summary>
        /// Give a table and value, find the item
        /// </summary>
        /// <param name="ctxt">Database connection</param>
        /// <param name="tableId">The table ID</param>
        /// <param name="valueId">The value ID of the primary key</param>
        /// <param name="noCreate">Whether to return -1 on error</param>
        /// <returns>Database row ID</returns>
        public static async Task<long> GetIdAsync(Context ctxt, int tableId, long valueId, bool noCreate = false)
        {
            var cmdParams =
                new Dictionary<string, object>
                {
                    { "@tableId", tableId },
                    { "@valueId", valueId }
                };
            string selectSql = "SELECT id FROM items WHERE tableId = @tableId AND valueId = @valueId";
            object idObj = await ctxt.Db.ExecuteScalarAsync(selectSql, cmdParams).ConfigureAwait(false);
            long id = Utils.ConvertDbInt64(idObj);
            
            if (noCreate && id < 0)
                return -1;
            else if (id >= 0)
                return id;

            string insertSql = $"{ctxt.Db.InsertIgnore} items (tableid, valueid, created, lastmodified) " +
                                $"VALUES (@tableId, @valueId, {ctxt.Db.UtcTimestampFunction}, {ctxt.Db.UtcTimestampFunction})";
            id = await ctxt.Db.ExecuteInsertAsync(insertSql, cmdParams).ConfigureAwait(false);
            return id;
        }

        /// <summary>
        /// Given an item ID, get the name-to-value metadata for the item
        /// </summary>
        /// <param name="ctxt">Database connection</param>
        /// <param name="itemId">The item to get metadat for</param>
        /// <returns>name_id-to-value_id mapping</returns>
        public static async Task<Dictionary<int, long>> GetItemDataAsync(Context ctxt, long itemId)
        {
            var retVal = new Dictionary<int, long>();
            string sql = $"SELECT nameid, valueid FROM itemnamevalues WHERE itemid = {itemId}";
            using (var reader = await ctxt.Db.ExecuteReaderAsync(sql).ConfigureAwait(false))
            {
                while (await reader.ReadAsync().ConfigureAwait(false))
                    retVal[reader.GetInt32(0)] = reader.GetInt64(1);
            }
            return retVal;
        }

        /// <summary>
        /// Given an item put name=>value metadata into the database
        /// </summary>
        /// <param name="ctxt">Database connection</param>
        /// <param name="itemId">The item to add metadata to</param>
        /// <param name="itemData">The name=>value ID metadata</param>
        public static async Task SetItemDataAsync(Context ctxt, long itemId, Dictionary<int, long> itemData)
        {
            string updateSql = $"UPDATE items SET lastmodified = {ctxt.Db.UtcTimestampFunction} WHERE id = {itemId}";
            await ctxt.RunSqlAsync(updateSql).ConfigureAwait(false);

            foreach (var kvp in itemData)
            {
                string sql;
                if (kvp.Value >= 0) // add-or-update it
                {
                    sql =
                        $"INSERT INTO itemnamevalues (itemid, nameid, valueid) " +
                        $"VALUES ({itemId}, {kvp.Key}, {kvp.Value}) " +
                        $"ON CONFLICT(itemid, nameid) " +
                        $"DO UPDATE SET valueid = {kvp.Value}";
                }
                else // remove it
                {
                    sql = $"DELETE FROM itemnamevalues WHERE itemid = {itemId} AND nameid = {kvp.Key}";
                }
                await ctxt.RunSqlAsync(sql).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Delete an item
        /// </summary>
        /// <param name="ctxt">Database connection</param>
        /// <param name="itemId">The item to delete</param>
        public static async Task DeleteAsync(Context ctxt, long itemId)
        {
            string sql = $"DELETE FROM items WHERE id = {itemId}";
            await ctxt.Db.ExecuteSqlAsync(sql).ConfigureAwait(false);
        }

        /// <summary>
        /// Get a summary of an item and its metadata
        /// </summary>
        /// <param name="ctxt">Database connection</param>
        /// <param name="itemId">The item to summarize</param>
        /// <returns>Summary of item</returns>
        public static async Task<string> SummarizeItemAsync(Context ctxt, long itemId)
        {
            var sb = new StringBuilder();

            sb.AppendLine($"Item: {itemId}");

            int tableId =
                Utils.ConvertDbInt32(await ctxt.Db.ExecuteScalarAsync($"SELECT tableid FROM items WHERE id = {itemId}"));
            if (tableId < 0)
                throw new FourDbException("Item not found: " + itemId);

            string tableName = (await Tables.GetTableAsync(ctxt, tableId)).name;
            sb.AppendLine($"Table: {tableName} ({tableId})");

            long valueId = 
                Utils.ConvertDbInt64(await ctxt.Db.ExecuteScalarAsync($"SELECT valueid FROM items WHERE id = {itemId}"));
            object value = await Values.GetValueAsync(ctxt, valueId);
            sb.AppendLine($"Value: {value} ({valueId})\n");

            sb.AppendLine("Metadata:");
            var metadata = await NameValues.GetMetadataValuesAsync(ctxt, GetItemDataAsync(ctxt, itemId).Result);
            foreach (var kvp in metadata)
                sb.AppendLine($"{kvp.Key}: {kvp.Value}");

            return sb.ToString();
        }
    }
}
