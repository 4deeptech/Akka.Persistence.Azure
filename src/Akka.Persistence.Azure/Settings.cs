using Akka.Configuration;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Akka.Persistence.Azure
{
    public class AzureStorageSettings
    {
        private Dictionary<string, CloudStorageAccount> _storageAccounts;

        public AzureStorageSettings(IList<string> connectionStrings)
        {
            _storageAccounts = new Dictionary<string, CloudStorageAccount>();
            for(int i=0;i<connectionStrings.Count;i++)
            {
                _storageAccounts.Add(i.ToString("X1"), CloudStorageAccount.Parse(connectionStrings[i]));
            }
        }

        public CloudStorageAccount GetAccount(string key)
        {
            if (_storageAccounts.Keys.Contains(key))
            {
                return _storageAccounts[key];
            }
            else return _storageAccounts["0"];
        }

        public CloudTableClient GetTableClient(string id)
        {
            return GetAccount(id.Substring(0, 1)).CreateCloudTableClient();
        }

        public CloudBlobClient GetBlobClient(string id)
        {
            return GetAccount(id.Substring(0, 1)).CreateCloudBlobClient();
        }

        public IEnumerable<CloudStorageAccount> GetStorageAccounts()
        {
            return _storageAccounts.Values;
        }
    }
    /// <summary>
    /// Configuration settings representation targeting Azure TableStorage journal actor.
    /// </summary>
    public class JournalSettings 
    {
        /// <summary>
        /// Name of the table corresponding to snapshot store.
        /// </summary>
        public string TableName { get; private set; }

        public IList<string> ConnectionStrings = new List<string>();

        private AzureStorageSettings _settings = null;

        public JournalSettings(Config config)
        {
            if (config == null) throw new ArgumentNullException("config", "Table Storage journal settings cannot be initialized, because required HOCON section couldn't be found");
            TableName = config.GetString("table-name");
            ConnectionStrings = config.GetStringList("connection-strings");
            _settings = new AzureStorageSettings(ConnectionStrings);
        }

        public CloudTableClient GetClient(string id)
        {
            return _settings.GetTableClient(id);
        }
    }

    /// <summary>
    /// Configuration settings representation targeting Azure Table Storage snapshot store actor.
    /// </summary>
    public class TableSnapshotStoreSettings
    {
        /// <summary>
        /// Name of the table corresponding to snapshot store.
        /// </summary>
        public string TableName { get; private set; }

        public IList<string> ConnectionStrings = new List<string>();

        private AzureStorageSettings _settings = null;

        public TableSnapshotStoreSettings(Config config)
        {
            if (config == null) throw new ArgumentNullException("config", "Azure Table Storage snapshot store settings cannot be initialized, because required HOCON section couldn't be found");
            TableName = config.GetString("table-name");
            ConnectionStrings = config.GetStringList("connection-strings");
            _settings = new AzureStorageSettings(ConnectionStrings);
        }

        public CloudTableClient GetClient(string id)
        {
            return _settings.GetTableClient(id);
        }
    }

    /// <summary>
    /// Configuration settings representation targeting Azure Table Storage snapshot store actor.
    /// </summary>
    public class BlobSnapshotStoreSettings
    {
        /// <summary>
        /// Name of the table corresponding to snapshot store.
        /// </summary>
        public string ContainerName { get; private set; }

        public IList<string> ConnectionStrings = new List<string>();

        private AzureStorageSettings _settings = null;

        public BlobSnapshotStoreSettings(Config config)
        {
            if (config == null) throw new ArgumentNullException("config", "Azure Blob Storage snapshot store settings cannot be initialized, because required HOCON section couldn't be found");
            ContainerName = config.GetString("container-name");
            ConnectionStrings = config.GetStringList("connection-strings");
            _settings = new AzureStorageSettings(ConnectionStrings);
        }

        public CloudBlobClient GetBlobClient(string id)
        {
            return _settings.GetBlobClient(id);
        }
    }
}
