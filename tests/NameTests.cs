using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace fourdb
{
    [TestClass]
    public class NameTests
    {
        [TestMethod]
        public void TestNames()
        {
            using (var ctxt = TestUtils.GetCtxt())
            {
                int tableId = Tables.GetIdAsync(ctxt, "sometable").Result;

                int firstId = Names.GetIdAsync(ctxt, tableId, "foobar").Result;
                int secondId = Names.GetIdAsync(ctxt, tableId, "foobar").Result;
                Assert.AreEqual(firstId, secondId);

                NameObj name = Names.GetNameAsync(ctxt, secondId).Result;
                Assert.AreEqual("foobar", name.name);
                Assert.IsTrue(!name.isNumeric);
                Assert.AreEqual("foobar", Names.GetNameAsync(ctxt, secondId).Result.name);
                Assert.AreEqual(name.isNumeric, Names.GetNameIsNumericAsync(ctxt, firstId).Result);

                int thirdId = Names.GetIdAsync(ctxt, tableId, "bletmonkey", isNumeric: true).Result;
                int fourthId = Names.GetIdAsync(ctxt, tableId, "bletmonkey").Result;
                Assert.AreEqual(thirdId, fourthId);
                Assert.AreNotEqual(firstId, thirdId);

                NameObj name2 = Names.GetNameAsync(ctxt, fourthId).Result;
                Assert.AreEqual("bletmonkey", name2.name);
                Assert.IsTrue(name2.isNumeric);
                Assert.AreEqual("bletmonkey", Names.GetNameAsync(ctxt, fourthId).Result.name);
                Assert.AreEqual(name2.isNumeric, Names.GetNameIsNumericAsync(ctxt, fourthId).Result);
            }
        }
    }
}
