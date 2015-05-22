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
    internal static class AzureStorageInitializer
    {
        /// <summary>
        /// Initializes journal tables according 'table-name' 
        ///  values provided in 'akka.persistence.journal.table-storage' config.
        /// </summary>
        internal static void CreateJournalTables(IEnumerable<string> storageAccountConnectionStrings, string tableName)
        {
            foreach(string connectionString in storageAccountConnectionStrings)
            {
                CreateTable(connectionString,tableName);
            }
        }

        /// <summary>
        /// Initializes snapshot store related table according to 'table-name' 
        /// values provided in 'akka.persistence.snapshot-store.table-storage' config.
        /// </summary>
        internal static void CreateSnapshotStoreTables(IEnumerable<string> storageAccountConnectionStrings, string tableName)
        {
            foreach (string connectionString in storageAccountConnectionStrings)
            {
                CreateTable(connectionString, tableName);
            }
        }

        /// <summary>
        /// Initializes snapshot store related containers to 'container-name' 
        /// values provided in 'akka.persistence.snapshot-store.blob-storage' config.
        /// </summary>
        internal static void CreateSnapshotStoreContainer(IEnumerable<string> storageAccountConnectionStrings, string containerName)
        {
            foreach (string connectionString in storageAccountConnectionStrings)
            {
                CreateContainer(connectionString, containerName);
            }
        }

        /// <summary>
        /// Creates the table storage table
        /// </summary>
        /// <param name="connectionString">The storage account connection string.</param>
        /// <param name="tableName">The table name to create.</param>
        internal static void CreateTable(string connectionString,string tableName)
        {
            CloudTableClient tableClient = CloudStorageAccount.Parse(connectionString).CreateCloudTableClient();
            CloudTable table = tableClient.GetTableReference(tableName);
            table.CreateIfNotExists();
        }

        /// <summary>
        /// Creates the storage container for the snapshots
        /// </summary>
        /// <param name="connectionString">The storage account connection string.</param>
        /// <param name="containerName">The container name to create.</param>
        internal static void CreateContainer(string connectionString, string containerName)
        {
            CloudBlobClient blobClient = CloudStorageAccount.Parse(connectionString).CreateCloudBlobClient();
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);
            container.CreateIfNotExists();
        }
    }


}
