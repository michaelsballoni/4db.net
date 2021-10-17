using System;
using System.Collections.Generic;
using System.Linq;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace fourdb
{
    [TestClass]
    public class DefineTest
    {
        [TestMethod]
        public void TestDefine()
        {
            using (var ctxt = TestUtils.GetCtxt())
            {
                {
                    var define = new Define("fun", "some");
                    define.Set("num", 42).Set("str", "foobar").Set("multi", "blet\nmonkey");
                    ctxt.DefineAsync(define).Wait();
                }

                {
                    var define = new Define("fun", "another");
                    define.Set("num", 69).Set("str", "boofar").Set("multi", "ape\nagony");
                    ctxt.DefineAsync(define).Wait();
                }

                {
                    var define = new Define("fun", "yetsome");
                    define.Set("num", 19).Set("str", "playful").Set("multi", "balloni\nbeats");
                    ctxt.DefineAsync(define).Wait();
                }

                long itemId = Items.GetIdAsync(ctxt, Tables.GetIdAsync(ctxt, "fun").Result, Values.GetIdAsync(ctxt, "some").Result).Result;
                var itemData = NameValues.GetMetadataValuesAsync(ctxt, Items.GetItemDataAsync(ctxt, itemId).Result).Result;

                Assert.AreEqual(42.0, itemData["num"]);
                Assert.AreEqual("foobar", itemData["str"]);
                Assert.AreEqual("blet\nmonkey", itemData["multi"]);

                {
                    Select select =
                        Sql.Parse
                        (
                            "SELECT value, multi\n" +
                            $"FROM fun\n" +
                            "WHERE multi MATCHES @search"
                        );
                    select.AddParam("@search", "monkey");
                    using (var reader = ctxt.ExecSelectAsync(select).Result)
                    {
                        if (!reader.ReadAsync().Result)
                            Assert.Fail();

                        var val = reader.GetString(0);
                        var str = reader.GetString(1);

                        Assert.AreEqual("some", val);
                        Assert.AreEqual("blet\nmonkey", str);

                        if (reader.ReadAsync().Result)
                            Assert.Fail();
                    }
                }

                {
                    Define define = new Define("fun", "some");
                    define.Set("num", 43.0);
                    define.Set("str", null); // remove the metadata

                    ctxt.DefineAsync(define).Wait();
                }

                itemData = NameValues.GetMetadataValuesAsync(ctxt, Items.GetItemDataAsync(ctxt, itemId).Result).Result;
                Assert.IsTrue(!itemData.ContainsKey("str"));
                Assert.AreEqual(43.0, itemData["num"]);
                Assert.IsTrue(!itemData.ContainsKey("str"));

                ctxt.DeleteAsync("fun", new[] { "some", "another", "yetsome" }).Wait();

                {
                    Select select = new Select()
                    {
                        from = "fun",
                        select = new List<string> { "value" }
                    };
                    using (var reader = ctxt.ExecSelectAsync(select).Result)
                    {
                        if (reader.ReadAsync().Result) // should be gone
                            Assert.Fail();
                    }
                }

                {
                    Define numsFirst = new Define("numsFirst", 1);
                    numsFirst.Set("foo", 12);
                    numsFirst.Set("blet", "79");
                    ctxt.DefineAsync(numsFirst).Wait();

                    Define numsNext = new Define("numsFirst", 2);
                    numsNext.Set("foo", 15).Set("blet", "63");
                    ctxt.DefineAsync(numsNext).Wait();

                    Select select = Sql.Parse("SELECT value, foo, blet\nFROM numsFirst");
                    using (var reader = ctxt.ExecSelectAsync(select).Result)
                    {
                        while (reader.Read())
                        {
                            if (reader.GetInt64(0) == 1)
                            {
                                Assert.AreEqual(12, reader.GetDouble(1));
                                Assert.AreEqual("79", reader.GetString(2));
                            }
                            else if (reader.GetInt64(0) == 2)
                            {
                                Assert.AreEqual(15, reader.GetDouble(1));
                                Assert.AreEqual("63", reader.GetString(2));
                            }
                            else
                            {
                                Assert.Fail();
                            }
                        }
                    }
                }
            }
        }
    }
}
