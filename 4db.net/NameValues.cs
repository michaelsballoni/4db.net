using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace fourdb
{
    /// <summary>
    /// metastrings implementation class for the name-value metadata in the virtual schema
    /// </summary>
    public static class NameValues
    {
        /// <summary>
        /// Remove everying from the metastrings database
        /// Used by unit tests
        /// </summary>
        /// <param name="ctxt"></param>
        public static void Reset(Context ctxt)
        {
            Items.Reset(ctxt);

            Values.Reset(ctxt);
            Names.Reset(ctxt);
            Tables.Reset(ctxt);
        }

        /// <summary>
        /// Clear the object->id and id-object caches in the name-value classes
        /// </summary>
        public static void ClearCaches()
        {
            Names.ClearCaches();
            Tables.ClearCaches();
        }

        /// <summary>
        /// Given metadata name-value IDs, return name-value string->object values
        /// </summary>
        /// <param name="ctxt">Database connection</param>
        /// <param name="ids">name=>value IDs</param>
        /// <returns></returns>
        public static async Task<Dictionary<string, object>> GetMetadataValuesAsync(Context ctxt, Dictionary<int, long> ids)
        {
            var totalTimer = ScopeTiming.StartTiming();
            try
            {
                var retVal = new Dictionary<string, object>(ids.Count);
                if (ids.Count == 0)
                    return retVal;

                foreach (var kvp in ids)
                {
                    NameObj name = await Names.GetNameAsync(ctxt, kvp.Key).ConfigureAwait(false);
                    object value = await Values.GetValueAsync(ctxt, kvp.Value).ConfigureAwait(false);
                    retVal.Add(name.name, value);
                }
                return retVal;
            }
            finally
            {
                ScopeTiming.RecordScope("NameValues.GetMetadataValuesAsync", totalTimer);
            }
        }
    }
}
