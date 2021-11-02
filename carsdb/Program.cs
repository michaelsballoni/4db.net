using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.IO;

namespace fourdb
{
    /// <summary>
    /// This program demonstrates creating a NoSQL database and using all four of the supported commands
    /// to populate and manipulate a cars database.
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
            // we simply need to specify the location for the database file.
            // If the file does not exist, an empty database is automatically created.
            // The Context class manages the SQLite database connection,
            // provides many useful functions for executing SELECT queries,
            // and provides access to the Command class for UPSERT, DELETE, and DROP.
            using (var ctxt = new Context("cars.db"))
            {
                // Pass our Context into AddCarAsync to add database records...so many cars...
                Console.WriteLine("Adding cars...");
                await AddCarAsync(ctxt, 1983, "Toyota", "Tercel");
                await AddCarAsync(ctxt, 1998, "Toyota", "Tacoma");
                await AddCarAsync(ctxt, 2001, "Nissan", "Xterra");
                await AddCarAsync(ctxt, 1987, "Nissan", "Pathfinder");
                //...

                // Select data out of the database using a basic dialect of SQL
                // Restrictions:
                // 1. No JOINs
                // 2. WHERE criteria must use parameters
                // 3. ORDER BY colums must be in SELECT column list
                // Here we gather the "value" pseudo-column, the row ID created by the AddCarAsync function
                // We create a Select object with our SELECT query,
                // pass in the value for the @year parameter,
                // and use the Context.ExecSelectAsync function to execute the query.
                Console.WriteLine("Getting old cars...");
                var oldCarGuids = new List<string>();
                Select select = 
                    Sql.Parse
                    (
                        "SELECT value, year, make, model " +
                        "FROM cars " +
                        "WHERE year < @year " +
                        "ORDER BY year ASC"
                    );
                select.AddParam("@year", 1990);
                using (var reader = await ctxt.ExecSelectAsync(select))
                {
                    // The reader handed back is a System.Data.Common.DbDataReader,
                    // straight out of SQLite.
                    while (reader.Read())
                    {
                        // Collect the row ID GUID that AddCarAsync added.
                        oldCarGuids.Add(reader.GetString(0));

                        // 4db values are either numbers (doubles) or strings
                        Console.WriteLine
                        (
                            reader.GetDouble(1) + ": " + 
                            reader.GetString(2) + " - " + 
                            reader.GetString(3)
                        );
                    }
                }

                // We use the list of row IDs to delete some rows.
                Console.WriteLine("Deleting old cars...");
                await ctxt.DeleteAsync("cars", oldCarGuids);

                // Drop the table to keep things clean for the next run.
                Console.WriteLine("Cleaning up...");
                await ctxt.DropAsync("cars");

                Console.WriteLine("All done.");
            }
        }

        // Given info about a car, add it to the database using the Context object and a Define
        /// <summary>
        /// UPSERT a car into our database
        /// </summary>
        /// <param name="ctxt">The Context for doing database work</param>
        /// <param name="year">The year of the car</param>
        /// <param name="make">The make of the car</param>
        /// <param name="model">The model of the car</param>
        /// <returns></returns>
        static async Task AddCarAsync(Context ctxt, int year, string make, string model)
        {
            // The Define class is used to do UPSERTs.
            // You pass the table name and primary key value to the constructor.
            // No need to create tables, just refer to them and the database takes care of it.
            // The second parameter to the Define constructor is the row ID.
            // This would be a natural primary key, but lacking that we use a GUID.
            Define define = new Define("cars", Guid.NewGuid().ToString());

            // Use Define.Set function to add column data
            define.Set("year", year);
            define.Set("make", make);
            define.Set("model", model);

            // Call through the Context to create a Command and do the UPSERT
            await ctxt.DefineAsync(define);
        }
    }
}
