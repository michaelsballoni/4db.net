using System;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace fourdb
{
    [TestClass]
    public class TableTests
    {
        [TestMethod]
        public void TestTables()
        {
            using (var ctxt = TestUtils.GetCtxt())
            {
                for (int t = 1; t <= 3; ++t)
                {
                    int firstId = Tables.GetIdAsync(ctxt, "foobar", isNumeric: true).Result;
                    int secondId = Tables.GetIdAsync(ctxt, "foobar").Result;
                    Assert.AreEqual(firstId, secondId);

                    int thirdId = Tables.GetIdAsync(ctxt, "bletmonkey", isNumeric: true).Result;
                    int fourthId = Tables.GetIdAsync(ctxt, "bletmonkey").Result;
                    Assert.AreEqual(thirdId, fourthId);
                    Assert.AreNotEqual(firstId, thirdId);

                    using (var reader = ctxt.Db.ExecuteReaderAsync("SELECT * FROM tables").Result)
                    {
                        while (reader.Read())
                        {
                            object id = reader["id"];
                            object name = reader["name"];
                            Console.WriteLine("tables {0} - {1}", id, name);
                        }
                    }

                    TableObj table = Tables.GetTableAsync(ctxt, secondId).Result;
                    Assert.AreEqual("foobar", table.name);
                    Assert.IsTrue(table.isNumeric);

                    TableObj tables2 = Tables.GetTableAsync(ctxt, secondId).Result;
                    Assert.AreEqual("foobar", tables2.name);
                    Assert.IsTrue(tables2.isNumeric);

                    TableObj tables3 = Tables.GetTableAsync(ctxt, thirdId).Result;
                    Assert.AreEqual("bletmonkey", tables3.name);
                    Assert.IsTrue(tables3.isNumeric);

                    TableObj tables4 = Tables.GetTableAsync(ctxt, fourthId).Result;
                    Assert.AreEqual("bletmonkey", tables4.name);
                    Assert.IsTrue(tables4.isNumeric);
                }
            }
        }
    }
}
