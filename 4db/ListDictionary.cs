using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace fourdb
{
    /// <summary>
    /// A hybrid class, Dictionary plus a List used to track the date-added order for enumeration
    /// Add-only and read-only makes for clean code
    /// </summary>
    /// <typeparam name="K"></typeparam>
    /// <typeparam name="V"></typeparam>
    public class ListDictionary<K, V> : IEnumerable<KeyValuePair<K, V>>
    {
        /// <summary>
        /// Default constructor
        /// </summary>
        public ListDictionary()
        {
        }

        /// <summary>
        /// Copy constructor
        /// </summary>
        /// <param name="dict">ListDictionary to construct with</param>
        public ListDictionary(ListDictionary<K, V> dict)
        {
            foreach (var kvp in dict)
                Add(kvp.Key, kvp.Value);
        }

        /// <summary>
        /// Dictionary constructor
        /// </summary>
        /// <param name="dict">Dictionary to populate this with</param>
        public ListDictionary(Dictionary<K, V> dict)
        {
            foreach (var kvp in dict)
                Add(kvp.Key, kvp.Value);
        }

        /// <summary>
        /// Enumerate over the keys, in data-added order
        /// </summary>
        public IEnumerable<K> Keys => m_list.Select(kvp => kvp.Key);

        /// <summary>
        /// Enumerate over the values, in data-added order
        /// </summary>
        public IEnumerable<V> Values => m_list.Select(kvp => kvp.Value);

        /// <summary>
        /// Get a key by value, value = list_dictionary["key"]
        /// Uses the Dictionary for this lookup
        /// NOTE: You cannot modify the contents of this collection
        /// </summary>
        /// <param name="key">Key to look up the value for</param>
        /// <returns>Value found for key</returns>
        public V this[K key] => m_dict[key];

        /// <summary>
        /// How many entries are in this collection?
        /// </summary>
        public int Count => m_list.Count;

        /// <summary>
        /// Does this collection have a key?
        /// Uses the Dictionary for this lookup
        /// </summary>
        /// <param name="key">Key to look for</param>
        /// <returns>true if key found</returns>
        public bool ContainsKey(K key) => m_dict.ContainsKey(key);

        /// <summary>
        /// Add a key-value pair to this
        /// Adds to Dictionary and List
        /// Adds to the Dictionary first to catch duplicate key errors before they pollute the List
        /// </summary>
        /// <param name="key">Key to add</param>
        /// <param name="val">Value to add</param>
        public void Add(K key, V val)
        {
            m_dict.Add(key, val); 
            m_list.Add(new KeyValuePair<K, V>(key, val));
        }

        /// <summary>
        /// Try to get a value for a given key
        /// Uses the Dictionary for this lookup
        /// </summary>
        /// <param name="key">Key to look for</param>
        /// <param name="val">Value to set on success</param>
        /// <returns>Whether a value could be found for the key</returns>
        public bool TryGetValue(K key, out V val)
        {
            return m_dict.TryGetValue(key, out val);
        }

        /// <summary>
        /// Enumerate the data in data-added order
        /// Uses the List for data-added enumeration
        /// </summary>
        /// <returns>Enumerator</returns>
        public IEnumerator<KeyValuePair<K, V>> GetEnumerator()
        {
            return ((IEnumerable<KeyValuePair<K, V>>)m_list).GetEnumerator();
        }

        /// <summary>
        /// Enumerate the data in data-added order
        /// Uses the List for data-added enumeration
        /// </summary>
        /// <returns>Enumerator</returns>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable)m_list).GetEnumerator();
        }

        /// <summary>
        /// The ordered list of key-value pairs, used for enumerating in data-added order
        /// </summary>
        private List<KeyValuePair<K, V>> m_list = new List<KeyValuePair<K, V>>();

        /// <summary>
        /// The dictionary of keys and values, used for looking up values by key
        /// </summary>
        private Dictionary<K, V> m_dict = new Dictionary<K, V>();
    }
}
