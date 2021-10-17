using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;

namespace fourdb
{
    /// <summary>
    /// Utility functions for implementing metastrings
    /// </summary>
    public static class Utils
    {
        /// <summary>
        /// Convert a result from an Exec(ute)Scalar... call into a 64-bit integer
        /// </summary>
        /// <param name="obj">Return value from Exec(ute)Scalar to process</param>
        /// <returns>64-bit integer if obj processed, otherwise -1</returns>
        public static long ConvertDbInt64(object obj)
        {
            if (obj == null || obj == DBNull.Value)
                return -1;
            else
                return Convert.ToInt64(obj);
        }

        /// <summary>
        /// Convert a result from an Exec(ute)Scalar... call into a 32-bit integer
        /// </summary>
        /// <param name="obj">Return value from Exec(ute)Scalar to process</param>
        /// <returns>32-bit integer if obj processed, otherwise -1</returns>
        public static int ConvertDbInt32(object obj)
        {
            if (obj == null || obj == DBNull.Value)
                return -1;
            else
                return Convert.ToInt32(obj);
        }

        /// <summary>
        /// Turn a SQL statement into tokens
        /// </summary>
        /// <param name="sql">SQL statement to tokenize</param>
        /// <returns>parsed tokens</returns>
        public static string[] Tokenize(string sql)
        {
            string[] tokens = 
                sql.Split(new[] { ' ', '\n', '\r', '\t' }, StringSplitOptions.RemoveEmptyEntries);
            return tokens;
        }

        public static List<string> ExtractParamNames(string sql)
        {
            List<string> paramNames = new List<string>();
            StringBuilder sb = new StringBuilder();
            int lookFrom = 0;
            while (true)
            {
                if (lookFrom >= sql.Length)
                    break;

                int at = sql.IndexOf('@', lookFrom);
                if (at < 0)
                    break;

                sb.Clear();
                int idx = at + 1;
                while (idx < sql.Length)
                {
                    char c = sql[idx++];
                    if (char.IsLetterOrDigit(c) || c == '_')
                    {
                        sb.Append(c);
                    }
                    else
                    {
                        break;
                    }
                }

                if (sb.Length > 0)
                {
                    paramNames.Add($"@{sb}");
                    sb.Clear();
                }
                lookFrom = idx;
            }

            if (sb.Length > 0)
            {
                paramNames.Add($"@{sb}");
                sb.Clear();
            }
            return paramNames;
        }

        /// <summary>
        /// Make a name usable as a table or column alias
        /// </summary>
        /// <param name="name">Name to cleanse</param>
        /// <returns>Name of just numbers and letters, or just 'a'</returns>
        public static string CleanName(string name) // used for table and column aliases
        {
            string clean = "";
            foreach (char c in name)
            {
                if (char.IsLetterOrDigit(c))
                    clean += c;
            }

            if (string.IsNullOrWhiteSpace(clean) || !char.IsLetter(clean[0]))
                clean = "a" + clean;

            return clean;
        }

        /// <summary>
        /// Can a value be used for a Name or Table name?
        /// </summary>
        public static bool IsWord(string word)
        {
            if (string.IsNullOrWhiteSpace(word))
                return false;
            else
                return IsWordRegEx.IsMatch(word) && !word.EndsWith("_", StringComparison.Ordinal);
        }

        /// <summary>
        /// Can a value be used for the name of a parameter for a SQL query?
        /// </summary>
        public static bool IsParam(string param)
        {
            if (string.IsNullOrWhiteSpace(param))
                return false;
            else
                return IsParamRegEx.IsMatch(param) && !param.EndsWith("_", StringComparison.Ordinal);
        }

        /// <summary>
        /// Is a name something used internally by metastrings?
        /// </summary>
        public static bool IsNameReserved(string name)
        {
            return ReservedWords.Contains(name.ToLower());
        }

        /// <summary>
        /// Ensure a table name is valid in a SQL query
        /// </summary>
        /// <param name="table">Table name to validate</param>
        /// <param name="sql">SQL query the table name appears in</param>
        public static void ValidateTableName(string table, string sql)
        {
            if (!Utils.IsWord(table))
                throw new SqlException($"Invalid table name: {table}", sql);
        }

        /// <summary>
        /// Ensure a column name is valid in a SQL query
        /// </summary>
        /// <param name="col">Column name to validate</param>
        /// <param name="sql">SQL query the column name appears in</param>
        public static void ValidateColumnName(string col, string sql)
        {
            if (!Utils.IsWord(col))
                throw new SqlException($"Invalid column name: {col}", sql);
        }

        /// <summary>
        /// Ensure a parameter name is valid in a SQL query
        /// </summary>
        /// <param name="param">Parameter name to validate</param>
        /// <param name="sql">SQL query the parameter name appears in</param>
        public static void ValidateParameterName(string parm, string sql)
        {
            if (!Utils.IsParam(parm))
                throw new SqlException($"Invalid parameter name: {parm}", sql);
        }

        /// <summary>
        /// Ensure an operator name is valid in a SQL query
        /// </summary>
        /// <param name="op">Operator to validate</param>
        /// <param name="sql">SQL query the operator name appears in</param>
        public static void ValidateOperator(string op, string sql)
        {
            if (!QueryOps.Contains(op.ToLower()))
                throw new SqlException($"Invalid query operator: {op}", sql);
        }

        private const string WordPattern = "^[a-zA-Z](\\w)*$";
        private static Regex IsWordRegEx = new Regex(WordPattern, RegexOptions.Compiled);

        private const string ParamPattern = "^\\@[a-zA-Z](\\w)*$";
        private static Regex IsParamRegEx = new Regex(ParamPattern, RegexOptions.Compiled);

        public static readonly HashSet<string> QueryOps =
            new HashSet<string>
            {
                "=", "<>", ">", ">=", "<", "<=",
                "matches",
                "like"
            };

        public static readonly HashSet<string> ReservedWords =
            new HashSet<string> 
            { 
                "select",
                "from",
                "where",
                "limit",
                "value", 
                "id", 
                "count",
                "created",
                "lastmodified",
                "relevance"
            };
    }
}
