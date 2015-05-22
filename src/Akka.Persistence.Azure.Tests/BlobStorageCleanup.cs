using Akka.Persistence.Azure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Akka.Persistence.TableStorage.Tests
{
    public static class BlobStorageCleanup
    {
        public static void Clean(string containerName, IList<string> connectionStrings)
        {
            foreach (CloudStorageAccount conn in new AzureStorageSettings(connectionStrings).GetStorageAccounts())
            {
                IEnumerable<CloudBlockBlob> blobs = conn.CreateCloudBlobClient()
                    .GetContainerReference(containerName)
                    .ListBlobs(null, true, BlobListingDetails.All).Cast<CloudBlockBlob>();
                if (blobs.Count() > 0)
                {
                    foreach (CloudBlockBlob b in blobs)
                    {
                        b.DeleteIfExists();
                    }
                }
            }
        }
    }
}
