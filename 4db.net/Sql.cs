using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Text;
using System.Linq;

namespace fourdb
{
    /// <summary>
    /// API for turning SQL strings to and from query objects
    /// </summary>
    public static class Sql
    {
        private enum SqlState
        {
            SELECT,
            FROM,
            WHERE,
            ORDER,
            LIMIT
        }

        /// <summary>
        /// Given a SQL-like query, return a a Select object, ready for adding parameters and querying
        /// </summary>
        /// <param name="sql">SQL-like query</param>
        /// <returns>Select object for adding parameters and executing</returns>
        public static Select Parse(string sql)
        {
            string[] tokens = Utils.Tokenize(sql);
            if (tokens.Length == 0 || (tokens.Length == 1 && string.IsNullOrWhiteSpace(tokens[0])))
                throw new SqlException("No tokens", sql);

            Select select = new Select();
            SqlState state = SqlState.SELECT;
            int idx = 0;
            while (idx < tokens.Length)
            {
                string currentToken = tokens[idx];
                if (state == SqlState.SELECT)
                {
                    // Should start with SELECT
                    if (!currentToken.Equals("SELECT", StringComparison.OrdinalIgnoreCase))
                        throw new SqlException("No SELECT", sql);

                    // Slurp up the SELECT columns
                    select.select = new List<string>();
                    while (true)
                    {
                        ++idx;
                        if (idx >= tokens.Length)
                            throw new SqlException("No SELECT columns", sql);

                        currentToken = tokens[idx];

                        bool lastColumn = !currentToken.EndsWith(",", StringComparison.Ordinal);
                        if (!lastColumn)
                            currentToken = currentToken.TrimEnd(',');
                        
                        Utils.ValidateColumnName(currentToken, sql);
                        select.select.Add(currentToken);

                        if (lastColumn)
                            break;
                    }

                    ++idx;
                    state = SqlState.FROM;
                    continue;
                }

                if (state == SqlState.FROM)
                {
                    if (!currentToken.Equals("FROM", StringComparison.OrdinalIgnoreCase))
                        throw new SqlException("No FROM", sql);

                    ++idx;
                    if (idx >= tokens.Length)
                        throw new SqlException("No FROM table", sql);
                    currentToken = tokens[idx];
                    Utils.ValidateTableName(currentToken, sql);
                    select.from = currentToken;
                    ++idx;
                    state = SqlState.WHERE;
                    continue;
                }

                if (state == SqlState.WHERE)
                {
                    if (!currentToken.Equals("WHERE", StringComparison.OrdinalIgnoreCase))
                    {
                        state = SqlState.ORDER;
                        continue;
                    }

                    // Gobble up WHERE criteria
                    CriteriaSet criteriaSet = new CriteriaSet();
                    select.where = new List<CriteriaSet> { criteriaSet };
                    ++idx;
                    while ((idx + 3) <= tokens.Length)
                    {
                        var criteria =
                            new Criteria()
                            {
                                name = tokens[idx++],
                                op = tokens[idx++],
                                paramName = tokens[idx++]
                            };
                        Utils.ValidateColumnName(criteria.name, sql);
                        Utils.ValidateOperator(criteria.op, sql);
                        Utils.ValidateParameterName(criteria.paramName, sql);
                        criteriaSet.AddCriteria(criteria);

                        if
                        (
                            (idx + 3) <= tokens.Length
                            &&
                            tokens[idx].Equals("AND", StringComparison.OrdinalIgnoreCase)
                        )
                        {
                            ++idx;
                            continue;
                        }
                        else
                        {
                            break;
                        }
                    }

                    if (criteriaSet.criteria.Count == 0)
                        throw new SqlException("No WHERE criteria", sql);

                    state = SqlState.ORDER;
                    continue;
                }

                if (state == SqlState.ORDER)
                {
                    string nextToken = (idx + 1) < tokens.Length ? tokens[idx + 1] : "";
                    if
                    (
                        (idx + 3) > tokens.Length
                        ||
                        !currentToken.Equals("ORDER", StringComparison.OrdinalIgnoreCase)
                        ||
                        !nextToken.Equals("BY", StringComparison.OrdinalIgnoreCase)
                    )
                    {
                        state = SqlState.LIMIT;
                        continue;
                    }

                    idx += 2;

                    var orders = new List<Order>();
                    select.orderBy = orders;
                    while (idx < tokens.Length)
                    {
                        currentToken = tokens[idx];

                        bool currentEnds = idx == tokens.Length - 1 || currentToken.EndsWith(",", StringComparison.Ordinal);

                        nextToken = "ASC";
                        if (!currentEnds)
                        {
                            if ((idx + 1) < tokens.Length)
                                nextToken = tokens[++idx];
                        }

                        bool nextEnds = nextToken.EndsWith(",", StringComparison.Ordinal);

                        bool isLimit = nextToken.Equals("LIMIT", StringComparison.OrdinalIgnoreCase);

                        bool lastColumn = isLimit || !(currentEnds || nextEnds);

                        currentToken = currentToken.TrimEnd(',');
                        nextToken = nextToken.TrimEnd(',');

                        bool isDescending;
                        {
                            if (nextToken.Equals("ASC", StringComparison.OrdinalIgnoreCase))
                                isDescending = false;
                            else if (nextToken.Equals("DESC", StringComparison.OrdinalIgnoreCase))
                                isDescending = true;
                            else if (isLimit)
                                isDescending = false;
                            else
                                throw new SqlException("Invalid ORDER BY", sql);
                        }

                        Utils.ValidateColumnName(currentToken, sql);

                        var orderObj = new Order() { field = currentToken, descending = isDescending };
                        orders.Add(orderObj);

                        if (!isLimit)
                            ++idx;

                        if (lastColumn)
                            break;
                    }

                    state = SqlState.LIMIT;
                    continue;
                }

                if (state == SqlState.LIMIT)
                {
                    if (currentToken.Equals("LIMIT", StringComparison.OrdinalIgnoreCase))
                    {
                        ++idx;
                        if (idx >= tokens.Length)
                            throw new SqlException("No LIMIT value", sql);
                        currentToken = tokens[idx];

                        int limitVal;
                        if (!int.TryParse(currentToken, out limitVal))
                            throw new SqlException("Invalid LIMIT value", sql);
                        select.limit = limitVal;

                        ++idx;
                        break;
                    }
                    else
                    {
                        throw new SqlException("Invalid final statement", sql);
                    }
                }

                throw new SqlException($"Invalid SQL parser state: {state}", sql);
            }

            if (idx < tokens.Length - 1)
                throw new SqlException("Not all parsed", sql);

            if (select.select.Count == 0)
                throw new SqlException("No SELECT columns", sql);

            if (string.IsNullOrWhiteSpace(select.from))
                throw new SqlException("No FROM", sql);

            return select;
        }

        /// <summary>
        /// This is where the magic 4db query => Database SQL query conversion takes place
        /// </summary>
        /// <param name="ctxt">Database connection</param>
        /// <param name="query">4db SQL query</param>
        /// <returns>Database SQL</returns>
        public static async Task<string> GenerateSqlAsync(Context ctxt, Select query)
        {
            //
            // "COMPILE"
            //
            if (string.IsNullOrWhiteSpace(query.from))
                throw new FourDbException("Invalid query, FROM is missing");

            if (query.select == null || query.select.Count == 0)
                throw new FourDbException("Invalid query, SELECT is empty");

            if (query.orderBy != null)
            {
                foreach (var order in query.orderBy)
                {
                    string orderField = order.field.Trim();
                    if (!query.select.Contains(orderField))
                    {
                        throw
                            new FourDbException
                            (
                                "Invalid query, ORDER BY columns must be present in SELECT column list: " +
                                $"{order.field.Trim()}"
                            );
                    }
                }
            }

            if (query.where != null)
            {
                foreach (var criteriaSet in query.where)
                {
                    foreach (var criteria in criteriaSet.criteria)
                    {
                        Utils.ValidateColumnName(criteria.name, "WHERE");
                        Utils.ValidateOperator(criteria.op, "WHERE");
                        Utils.ValidateParameterName(criteria.paramName, "WHERE");
                    }
                }
            }


            //
            // SETUP
            //
            int tableId = await Tables.GetIdAsync(ctxt, query.from, noCreate: true, noException: true).ConfigureAwait(false);
            TableObj tableObj = await Tables.GetTableAsync(ctxt, tableId).ConfigureAwait(false);

            // Gather columns
            var names = new List<string>();

            names.AddRange(query.select);

            if (query.orderBy != null)
                names.AddRange(query.orderBy.Select(o => o.field));

            if (query.where != null)
            {
                foreach (var criteriaSet in query.where)
                    names.AddRange(criteriaSet.criteria.Select(w => w.name));
            }

            // Cut them down
            names = names.Select(n => n.Trim()).Where(n => !string.IsNullOrEmpty(n)).Distinct().ToList();

            // Get name objects
            var nameObjs = new Dictionary<string, NameObj>(names.Count);
            foreach (var name in names)
            {
                if (Utils.IsNameReserved(name))
                {
                    nameObjs.Add(name, null);
                }
                else
                {
                    NameObj nameObj;
                    {
                        int nameId = await Names.GetIdAsync(ctxt, tableId, name, noCreate: true, noException: true).ConfigureAwait(false);
                        if (nameId < 0)
                            nameObj = null;
                        else
                            nameObj = await Names.GetNameAsync(ctxt, nameId);
                    }
                    nameObjs.Add(name, nameObj);
                }
            }


            //
            // SELECT
            //
            string selectPart = "";
            foreach (var name in query.select.Select(n => n.Trim()).Where(n => !string.IsNullOrWhiteSpace(n)))
            {
                var cleanName = Utils.CleanName(name);

                if (selectPart.Length > 0)
                    selectPart += ",\r\n";

                if (name == "value")
                {
                    if (tableObj == null)
                        selectPart += "NULL";
                    else if (tableObj.isNumeric)
                        selectPart += "bv.numberValue";
                    else
                        selectPart += "bv.stringValue";
                }
                else if (name == "id")
                    selectPart += "i.id";
                else if (name == "created")
                    selectPart += "i.created";
                else if (name == "lastmodified")
                    selectPart += "i.lastmodified";
                else if (name == "count")
                    selectPart += "COUNT(*)";
                else if (nameObjs[name] == null)
                    selectPart += "NULL";
                else if (nameObjs[name].isNumeric)
                    selectPart += $"iv{cleanName}.numberValue";
                else
                    selectPart += $"iv{cleanName}.stringValue";
                selectPart += $" AS {cleanName}";
            }
            selectPart = "SELECT\r\n" + selectPart;


            //
            // FROM
            //
            string fromPart = "FROM\r\nitems AS i";
            if (nameObjs.ContainsKey("value"))
                fromPart += "\r\nJOIN bvalues bv ON bv.id = i.valueid";

            foreach (var name in names.Select(n => n.Trim()).Where(n => !string.IsNullOrWhiteSpace(n)))
            {
                if (!Utils.IsNameReserved(name) && nameObjs.ContainsKey(name) && nameObjs[name] != null)
                {
                    var cleanName = Utils.CleanName(name);
                    fromPart +=
                        $"\r\nLEFT OUTER JOIN itemvalues AS iv{cleanName} ON iv{cleanName}.itemid = i.id" +
                        $" AND iv{cleanName}.nameid = {nameObjs[name].id}";
                }
            }


            //
            // WHERE
            //
            string wherePart = $"i.tableid = {tableId}";
            if (query.where != null)
            {
                foreach (var criteriaSet in query.where)
                {
                    if (criteriaSet.criteria.Count == 0)
                        continue;

                    wherePart += "\r\nAND\r\n";

                    wherePart += "(";
                    bool addedOneYet = false;
                    foreach (var where in criteriaSet.criteria)
                    {
                        string name = where.name.Trim();
                        if (string.IsNullOrWhiteSpace(name))
                            continue;

                        if (!addedOneYet)
                            addedOneYet = true;
                        else
                            wherePart += $" {Enum.GetName(criteriaSet.combine.GetType(), criteriaSet.combine)} ";

                        var nameObj = nameObjs[name];
                        var cleanName = Utils.CleanName(name);

                        if (where.op.Equals("matches", StringComparison.OrdinalIgnoreCase))
                        {
                            if (nameObj == null)
                            {
                                wherePart += "1 = 0"; // name doesn't exist, no match!
                            }
                            else
                            {
                                string matchTableLabel = cleanName == "value" ? "bvtValue" : $"bvt{cleanName}";
                                string matchColumnLabel = cleanName == "value" ? "i.valueid" : $"iv{cleanName}.valueid";
                                
                                fromPart += $" JOIN bvaluetext {matchTableLabel} ON {matchTableLabel}.valueid = {matchColumnLabel}";
                                
                                wherePart += $"{matchTableLabel}.stringSearchValue MATCH {where.paramName}";
                                
                                if (query.orderBy == null)
                                    query.orderBy = new List<Order>();
                                query.orderBy.Add(new Order() { field = "rank" });
                            }
                        }
                        else if (cleanName == "id")
                        {
                            wherePart += $"i.id {where.op} {where.paramName}";
                        }
                        else if (cleanName == "value")
                        {
                            if (tableObj == null)
                                wherePart += "1 = 0"; // no table, no match
                            else if (tableObj.isNumeric)
                                wherePart += $"bv.numberValue {where.op} {where.paramName}";
                            else
                                wherePart += $"bv.stringValue {where.op} {where.paramName}";
                        }
                        else if (cleanName == "created" || cleanName == "lastmodified")
                        {
                            wherePart += $"{cleanName} {where.op} {where.paramName}";
                        }
                        else if (nameObj == null)
                        {
                            wherePart += "1 = 0"; // name doesn't exist, no match!
                        }
                        else if (nameObj.isNumeric)
                        {
                            wherePart += $"iv{cleanName}.numberValue {where.op} {where.paramName}";
                        }
                        else
                        {
                            wherePart += $"iv{cleanName}.stringValue {where.op} {where.paramName}";
                        }
                    }
                    wherePart += ")";
                }
            }
            wherePart = "WHERE " + wherePart;


            //
            // ORDER BY
            //
            string orderBy = "";
            if (query.orderBy != null)
            {
                foreach (var order in query.orderBy)
                {
                    if (string.IsNullOrWhiteSpace(order.field))
                        continue;

                    if (orderBy.Length > 0)
                        orderBy += ",\r\n";

                    string orderColumn = order.field;
                    if (!Utils.IsNameReserved(order.field))
                        Utils.CleanName(order.field);

                    orderBy += orderColumn + (order.descending ? " DESC" : " ASC");
                }

                if (orderBy.Length > 0)
                    orderBy = "ORDER BY\r\n" + orderBy;
            }


            //
            // LIMIT
            //
            string limitPart = "";
            if (query.limit > 0)
                limitPart = $"LIMIT\r\n{query.limit}";


            //
            // SQL
            //
            StringBuilder sb = new StringBuilder();

            sb.Append($"{selectPart.Trim()}");

            sb.Append($"\r\n\r\n{fromPart}");

            if (!string.IsNullOrWhiteSpace(wherePart))
            {
                sb.Append($"\r\n\r\n{wherePart}");
            }

            if (!string.IsNullOrWhiteSpace(orderBy))
            {
                sb.Append($"\r\n\r\n{orderBy}");
            }

            if (!string.IsNullOrWhiteSpace(limitPart))
            {
                sb.Append($"\r\n\r\n{limitPart}");
            }

            string sql = sb.ToString();
            return sql;
        }
    }
}
