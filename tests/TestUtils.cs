using System;
using System.IO;

namespace fourdb
{
    public static class TestUtils
    {
        public static Context GetCtxt() // start from scratch
        {
            string dbFilePath = Context.DbConnStrToFilePath("Data Source=[UserRoaming]/fourdb-tests.db");
            if (File.Exists(dbFilePath))
                File.Delete(dbFilePath);

            var ctxt = new Context("Data Source=" + dbFilePath);
            NameValues.Reset(ctxt);
            return ctxt;
        }
    }
}
