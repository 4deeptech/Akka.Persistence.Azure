using Akka.Persistence.Azure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Akka.Persistence.TableStorage.Tests
{
    public static class TableStorageCleanup
    {
        public static void Clean(string tableName, IList<string> connectionStrings)
        {
            AzureStorageSettings settings =
                new AzureStorageSettings(connectionStrings);
            foreach (CloudStorageAccount conn in settings.GetStorageAccounts())
            {
                CloudTableClient tableClient = conn.CreateCloudTableClient();
                CloudTable table = tableClient.GetTableReference(tableName);
                TableQuery<DynamicTableEntity> query =
                        new TableQuery<DynamicTableEntity>();
                IEnumerable<DynamicTableEntity> results = table.ExecuteQuery(query);
                if (results.Count() > 0)
                {
                    TableBatchOperation batchOperation = new TableBatchOperation();
                    foreach (DynamicTableEntity s in results)
                    {
                        batchOperation.Delete(s);
                    }
                    table.ExecuteBatch(batchOperation);
                }

            }
        }
    }
}
