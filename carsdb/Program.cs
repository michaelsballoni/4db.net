using System;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace fourdb
{
    /// <summary>
    /// This program demonstrates creating a NoSQL database 
    /// and using all four of the supported commands to 
    /// populate and manipulate a database storing a catalog 
    /// of cars:
    /// 1. UPSERT
    /// 2. SELECT
    /// 3. DELETE
    /// 4. DROP
    /// </summary>
    class Program
    {
        static async Task Main()
        {
            // 4db is built on SQLite, so to create a 4db database
            // we simply need to specify the location of the database file;
            // if the file does not exist, an empty database is automatically created.
            // The Context class manages the SQLite database connection,
            // provides many useful functions for executing SELECT queries,
            // and implements the UPSERT, DELETE, and DROP functions.
            Console.WriteLine("Opening database...");
            using (var ctxt = new Context("cars.db"))
            {
				// Drop the table to start things clean for this run.
                Console.WriteLine("Starting up...");
                await ctxt.DropAsync("cars");

                // Pass our Context into AddCarAsync to add database records...so many cars...
                Console.WriteLine("Adding cars...");
                await AddCarAsync(ctxt, 1987, "Nissan", "Pathfinder");
                await AddCarAsync(ctxt, 1998, "Toyota", "Tacoma");
                await AddCarAsync(ctxt, 2001, "Nissan", "Xterra");
                //...

                // Select data out of the database using a basic dialect of SQL.
                // Restrictions:
                // 1. No JOINs
                // 2. WHERE criteria must use parameters
                // 3. ORDER BY colums must be in SELECT column list
                // Here we gather the "value" pseudo-column, the primary key,
                // created by the AddCarAsync function.
                // We create a Select object with our SELECT query,
                // pass in the value for the @year parameter,
                // and use the Context.ExecSelectAsync function to execute the query.
                Console.WriteLine("Getting old cars...");
                var oldCarKeys = new List<string>();
                Select select = 
                    Sql.Parse
                    (
                        "SELECT value, year, make, model " +
                        "FROM cars " +
                        "WHERE year < @year " +
                        "ORDER BY year ASC"
                    );
                select.AddParam("@year", 2000);
                using (var reader = await ctxt.ExecSelectAsync(select))
                {
                    // NOTE: reader is a System.Data.Common.DbDataReader straight out of SQLite
                    while (reader.Read())
                    {
                        // Collect the primary key that AddCarAsync added
                        oldCarKeys.Add(reader.GetString(0));

                        // 4db values are either numbers (doubles) or strings
                        Console.WriteLine
                        (
                            reader.GetDouble(1) + ": " + 
                            reader.GetString(2) + " - " + 
                            reader.GetString(3)
                        );
                    }
                }

                // We use the list of primary keys to delete some rows.
                Console.WriteLine("Deleting old cars...");
                await ctxt.DeleteAsync("cars", oldCarKeys);

                Console.WriteLine("All done.");
            }
        }

        /// <summary>
        /// UPSERT a car into our database using the define function.
        /// You pass the table name, primary key value, and column data to this function.
        /// No need to explicitly create tables, just refer to them and define takes care of it.
        /// NOTE: The primary key value and column data values
        ///       can only be strings or numbers.
        ///       For numbers, they have to be convertible to doubles,
        ///       and are selected out of the database as doubles.
        /// </summary>
        /// <param name="ctxt">The Context for doing database work</param>
        /// <param name="year">The year of the car</param>
        /// <param name="make">The make of the car</param>
        /// <param name="model">The model of the car</param>
        static async Task AddCarAsync(Context ctxt, int year, string make, string model)
        {
            string tableName = "cars";
            object primaryKey = year + "_" + make + "_" + model;
            var columnData = new Dictionary<string, object>
            {
                { "year", year },
                { "make", make },
                { "model", model },
            };
            await ctxt.DefineAsync(tableName, primaryKey, columnData);
        }
    }
}
