using System;
using System.Collections.Generic;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace fourdb
{
    [TestClass]
    public class UtilsTests
    {
        [TestMethod]
        public void TestIsWord()
        {
            Assert.IsTrue(Utils.IsWord("f"));
            Assert.IsTrue(Utils.IsWord("f2"));
            Assert.IsTrue(Utils.IsWord("f_2"));
            Assert.IsTrue(Utils.IsWord("foobar"));
            Assert.IsTrue(Utils.IsWord("foo_bar"));
            Assert.IsTrue(Utils.IsWord("foo_bar_12"));

            Assert.IsTrue(!Utils.IsWord(null));
            Assert.IsTrue(!Utils.IsWord(""));
            Assert.IsTrue(!Utils.IsWord(" "));
            Assert.IsTrue(!Utils.IsWord("@foo_bar_12"));
            Assert.IsTrue(!Utils.IsWord("foo_"));
            Assert.IsTrue(!Utils.IsWord("1foo_bar_12"));
            Assert.IsTrue(!Utils.IsWord("_foo_bar_12"));
            Assert.IsTrue(!Utils.IsWord("foo_bar_12_"));
        }

        [TestMethod]
        public void TestIsParam()
        {
            Assert.IsTrue(Utils.IsParam("@f"));
            Assert.IsTrue(Utils.IsParam("@f2"));
            Assert.IsTrue(Utils.IsParam("@foobar"));
            Assert.IsTrue(Utils.IsParam("@foo_bar"));
            Assert.IsTrue(Utils.IsParam("@foo_bar_12"));

            Assert.IsTrue(!Utils.IsParam(null));
            Assert.IsTrue(!Utils.IsParam(""));
            Assert.IsTrue(!Utils.IsParam(" "));
            Assert.IsTrue(!Utils.IsParam("f"));
            Assert.IsTrue(!Utils.IsParam("foo"));
            Assert.IsTrue(!Utils.IsParam("1foo_bar_12"));
            Assert.IsTrue(!Utils.IsParam("_foo_bar_12"));
            Assert.IsTrue(!Utils.IsParam("@foo_bar_12_"));
        }

        [TestMethod]
        public void TestCleanName()
        {
            Assert.AreEqual("a", Utils.CleanName(""));
            Assert.AreEqual("a", Utils.CleanName("%*&%*"));
            Assert.AreEqual("a1", Utils.CleanName("1"));
            Assert.AreEqual("b", Utils.CleanName("b"));
            Assert.AreEqual("dbc", Utils.CleanName("d_bc!"));
        }

        [TestMethod]
        public void TestDnConverts()
        {
            Assert.AreEqual(-1, Utils.ConvertDbInt32(null));
            Assert.AreEqual(-1, Utils.ConvertDbInt32(DBNull.Value));
            Assert.AreEqual(-1, Utils.ConvertDbInt32(-1));
            Assert.AreEqual(0, Utils.ConvertDbInt32(0));
            Assert.AreEqual(1, Utils.ConvertDbInt32("1"));
            Assert.AreEqual(1, Utils.ConvertDbInt32(1));

            Assert.AreEqual(-1, Utils.ConvertDbInt64(null));
            Assert.AreEqual(-1, Utils.ConvertDbInt64(DBNull.Value));
            Assert.AreEqual(-1, Utils.ConvertDbInt64(-1));
            Assert.AreEqual(0, Utils.ConvertDbInt64(0));
            Assert.AreEqual(1, Utils.ConvertDbInt64("1"));
            Assert.AreEqual(1, Utils.ConvertDbInt64(1));
        }

        [TestMethod]
        public void TestTokenize()
        {
            string sql;
            IEnumerable<string> tokens;

            sql = "";
            tokens = Utils.Tokenize(sql);
            Assert.AreEqual("", string.Join('|', tokens));

            sql = " ";
            tokens = Utils.Tokenize(sql);
            Assert.AreEqual("", string.Join('|', tokens));

            sql = "SELECT";
            tokens = Utils.Tokenize(sql);
            Assert.AreEqual("SELECT", string.Join('|', tokens));

            sql = " SELECT";
            tokens = Utils.Tokenize(sql);
            Assert.AreEqual("SELECT", string.Join('|', tokens));

            sql = "SELECT ";
            tokens = Utils.Tokenize(sql);
            Assert.AreEqual("SELECT", string.Join('|', tokens));

            sql = "SELECT foo";
            tokens = Utils.Tokenize(sql);
            Assert.AreEqual("SELECT|foo", string.Join('|', tokens));

            sql = "SELECT\nfoo";
            tokens = Utils.Tokenize(sql);
            Assert.AreEqual("SELECT|foo", string.Join('|', tokens));

            sql = "SELECT foo, bar";
            tokens = Utils.Tokenize(sql);
            Assert.AreEqual("SELECT|foo,|bar", string.Join('|', tokens));

            sql = "SELECT\n\tfoo";
            tokens = Utils.Tokenize(sql);
            Assert.AreEqual("SELECT|foo", string.Join('|', tokens));

            sql = "SELECT foo, bar\nFROM blet\n";
            tokens = Utils.Tokenize(sql);
            Assert.AreEqual("SELECT|foo,|bar|FROM|blet", string.Join('|', tokens));

            sql = "SELECT foo, bar FROM blet";
            tokens = Utils.Tokenize(sql);
            Assert.AreEqual("SELECT|foo,|bar|FROM|blet", string.Join('|', tokens));

            sql = "SELECT foo, bar\nFROM blet\nWHERE monkey = @something";
            tokens = Utils.Tokenize(sql);
            Assert.AreEqual("SELECT|foo,|bar|FROM|blet|WHERE|monkey|=|@something", string.Join('|', tokens));

            sql = "SELECT foo, bar FROM blet WHERE monkey = @something";
            tokens = Utils.Tokenize(sql);
            Assert.AreEqual("SELECT|foo,|bar|FROM|blet|WHERE|monkey|=|@something", string.Join('|', tokens));
        }

        [TestMethod]
        public void TestExtractParamNames()
        {
            string sql;
            IEnumerable<string> paramNames;

            sql = "";
            paramNames = Utils.ExtractParamNames(sql);
            Assert.AreEqual("", string.Join('|', paramNames));

            sql = "fred";
            paramNames = Utils.ExtractParamNames(sql);
            Assert.AreEqual("", string.Join('|', paramNames));

            sql = "fred something barney";
            paramNames = Utils.ExtractParamNames(sql);
            Assert.AreEqual("", string.Join('|', paramNames));

            sql = "@fred something barney";
            paramNames = Utils.ExtractParamNames(sql);
            Assert.AreEqual("@fred", string.Join('|', paramNames));

            sql = "fred something @barney";
            paramNames = Utils.ExtractParamNames(sql);
            Assert.AreEqual("@barney", string.Join('|', paramNames));

            sql = "@fred something @barney";
            paramNames = Utils.ExtractParamNames(sql);
            Assert.AreEqual("@fred|@barney", string.Join('|', paramNames));

            sql = "@fred @something @barney";
            paramNames = Utils.ExtractParamNames(sql);
            Assert.AreEqual("@fred|@something|@barney", string.Join('|', paramNames));
        }
    }

    public class TestSerializeType
    {
        public int foo { get; set; }
        public string bar { get; set; }
    }
}
