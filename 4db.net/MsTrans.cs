using System;

namespace fourdb
{
    /// <summary>
    /// Wrapper class for transactions
    /// This class manages transaction lifetime and nested trasactions.
    /// </summary>
    public class MsTrans : IDisposable
    {
        public MsTrans(IDb db)
        {
            m_db = db;
            ++m_db.TransCount; // we've begun
        }

        public void Dispose()
        {
            if (m_db.TransCount <= 0)
                throw new FourDbException($"Invalid transaction count (Dispose): {m_db.TransCount}");
            
            --m_db.TransCount;

            if (m_db.TransCount == 0)
                m_db.FreeTrans();
        }

        public void Commit()
        {
            if (m_db.TransCount <= 0)
                throw new FourDbException($"Invalid transaction count (Commit): {m_db.TransCount}");

            if (m_db.TransCount == 1)
                m_db.Commit();
        }

        private IDb m_db;
    }
}
