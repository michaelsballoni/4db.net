using System;
using System.Threading.Tasks;
using System.IO;
using System.Collections.Generic;

namespace fourdb
{
    class Program
    {
        static StreamWriter sm_sw = null;

        static void WriteLine(string str)
        {
            Console.WriteLine(str);
            if (sm_sw != null)
                sm_sw.WriteLine(str);
        }

        static async Task Main()
        {
            Console.WriteLine("Enter your SQL on one or more lines, then answer any param values");
            while (true)
            {
                try
                {
                    Console.WriteLine();
                    Console.Write("> ");
                    string query = Console.ReadLine();
                    if (string.IsNullOrWhiteSpace(query))
                        continue;

                    string nextLine;
                    while ((nextLine = Console.ReadLine()).Length > 0)
                        query += "\n" + nextLine;

                    Select select = Sql.Parse(query);
                    List<string> paramNames = Utils.ExtractParamNames(query);
                    if (paramNames.Count > 0)
                    {
                        foreach (string paramName in paramNames)
                        {
                            Console.Write($"{paramName}: ");
                            string paramValue = Console.ReadLine();

                            double numValue;
                            if (double.TryParse(paramValue, out numValue))
                                select.AddParam(paramName, numValue);
                            else
                                select.AddParam(paramName, paramValue.Trim('\"'));
                        }
                        Console.WriteLine();
                    }

                    int resultCount = 0;
                    using (var ctxt = new Context("metaq.db"))
                    {
                        string sql = await Sql.GenerateSqlAsync(ctxt, select);

                        if (sm_sw != null)
                            sm_sw.Dispose();
                        sm_sw = new StreamWriter("metaq.log");
                        sm_sw.WriteLine(query);
                        sm_sw.WriteLine();
                        sm_sw.WriteLine("===");
                        sm_sw.WriteLine();
                        sm_sw.WriteLine(sql);
                        sm_sw.WriteLine();
                        sm_sw.WriteLine("===");
                        sm_sw.WriteLine();

                        using (var reader = await ctxt.Db.ExecuteReaderAsync(sql, select.cmdParams))
                        {
                            while (reader.Read())
                            {
                                for (int i = 0; i < reader.FieldCount; ++i)
                                    WriteLine($"{reader.GetName(i)}: { reader.GetValue(i)}");
                                WriteLine("===");
                                ++resultCount;
                            }
                        }
                    }

                    WriteLine($"Results: {resultCount}");

                    sm_sw.Dispose();
                    sm_sw = null;
                }
                catch (SqlException exp)
                {
                    WriteLine($"SQL EXCEPTION: {exp.Message}");
                }
                catch (MetaStringsException exp)
                {
                    WriteLine($"metastrings EXCEPTION: {exp.Message}");
                }
                catch (Exception exp)
                {
                    WriteLine($"Unhandled EXCEPTION: {exp}");
                }
            } // loop forever
        }
    }
}
