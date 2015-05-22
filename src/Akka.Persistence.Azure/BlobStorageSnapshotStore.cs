using System;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.Collections.Generic;
using System.Threading;
using Akka.Event;
using Akka.Persistence;
using Akka.Persistence.Serialization;
using Akka.Persistence.Snapshot;
using Akka.Serialization;
using Newtonsoft.Json;
using Microsoft.WindowsAzure.Storage.Table;
using Akka.Persistence.Azure;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Text.RegularExpressions;
using System.IO;

namespace BlobStorage.Persistence
{
    public class BlobStorageSnapshotStore : SnapshotStore
    {
        private ILoggingAdapter _log;
        private JsonSerializerSettings _settings;
        private readonly AzureStoragePersistenceExtension _extension;

        public BlobStorageSnapshotStore()
        {
            _extension = AzureStoragePersistence.Instance.Apply(Context.System);
            _log = Context.GetLogger();
            _settings = new JsonSerializerSettings();
            _settings.TypeNameHandling = TypeNameHandling.All;
        }
               
        protected override async Task<SelectedSnapshot> LoadAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            IEnumerable<CloudBlockBlob> blobs = _extension
                .BlobSnapshotStoreSettings
                .GetBlobClient(persistenceId)
                .GetContainerReference(_extension.BlobSnapshotStoreSettings.ContainerName)
                .ListBlobs(persistenceId,true,BlobListingDetails.All).Cast<CloudBlockBlob>();

            var results = blobs.OrderByDescending(t => SnapshotVersionHelper.Parse(t.Name));

            if (results.Count() > 0)
            {
                _log.Debug("Found {0} snapshots for {1}",results.Count(), persistenceId);
                foreach(CloudBlockBlob clob in results)
                {
                    if (SnapshotVersionHelper.Parse(clob.Name) <= criteria.MaxSequenceNr
                        && long.Parse(clob.Metadata["SnapshotTimestamp"]) <= criteria.MaxTimeStamp.Ticks)
                    {
                        using (var memoryStream = new MemoryStream())
                        {
                            clob.DownloadToStream(memoryStream);
                            var snapshot = JsonConvert.DeserializeObject(System.Text.Encoding.UTF8.GetString(memoryStream.ToArray()), 
                                typeof(SelectedSnapshot), _settings);
                            return (SelectedSnapshot)snapshot;
                        }
                    }
                }
                return null;
            }
            else return null;
        }

        protected override async Task SaveAsync(SnapshotMetadata metadata, object snapshot)
        {
            try
            {
                CloudBlobClient blobClient = _extension.BlobSnapshotStoreSettings.GetBlobClient(metadata.PersistenceId);
                CloudBlobContainer container = blobClient.GetContainerReference(_extension.BlobSnapshotStoreSettings.ContainerName);
                CloudBlockBlob blockBlob = container.GetBlockBlobReference(metadata.PersistenceId + "." + SnapshotVersionHelper.ToVersionKey(metadata.SequenceNr) + ".json");
                var snap = JsonConvert.SerializeObject(new SelectedSnapshot(metadata, snapshot), _settings);
                using (var memoryStream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(snap)))
                {
                    memoryStream.Position = 0;
                    blockBlob.Metadata.Add("SnapshotTimestamp", metadata.Timestamp.Ticks.ToString());
                    blockBlob.Metadata.Add("Version", SnapshotVersionHelper.ToVersionKey(metadata.SequenceNr));
                    blockBlob.UploadFromStream(memoryStream);
                }
            }
            catch (Exception ex)
            {
                _log.Error(ex, ex.Message);
            }
        }

        protected override void Saved(SnapshotMetadata metadata)
        {}

        protected override void Delete(SnapshotMetadata metadata)
        {
            try
            {
                IEnumerable<CloudBlockBlob> blobs = _extension
                .BlobSnapshotStoreSettings
                .GetBlobClient(metadata.PersistenceId)
                .GetContainerReference(_extension.BlobSnapshotStoreSettings.ContainerName)
                .ListBlobs(metadata.PersistenceId + "." + SnapshotVersionHelper.ToVersionKey(metadata.SequenceNr) + ".json", true, BlobListingDetails.All).Cast<CloudBlockBlob>();

                foreach (CloudBlockBlob clob in blobs)
                {
                    _log.Debug("Found snapshot to delete for {0}", metadata.PersistenceId);
                    clob.DeleteIfExists();
                }
            }
            catch (Exception ex)
            {
                _log.Error(ex, ex.Message);
            }
        }

        protected override void Delete(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            try
            {
                IEnumerable<CloudBlockBlob> blobs = _extension
                    .BlobSnapshotStoreSettings
                    .GetBlobClient(persistenceId)
                    .GetContainerReference(_extension.BlobSnapshotStoreSettings.ContainerName)
                    .ListBlobs(persistenceId, true, BlobListingDetails.All).Cast<CloudBlockBlob>();

                var results = blobs.OrderByDescending(t => SnapshotVersionHelper.Parse(t.Name));

                if (results.Count() > 0)
                {
                    
                    foreach (CloudBlockBlob clob in results)
                    {
                        if (SnapshotVersionHelper.Parse(clob.Name) <= criteria.MaxSequenceNr
                            && long.Parse(clob.Metadata["SnapshotTimestamp"]) <= criteria.MaxTimeStamp.Ticks)
                        {
                            _log.Debug("Found snapshot to remove for {0}", persistenceId);
                            clob.DeleteIfExists();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _log.Error(ex, ex.Message);
                throw;
            }
        }

        #region Internal Snapshot helper

        private class SnapshotVersionHelper
        {
            public static long Parse(string blobName)
            {
                return long.Parse(Regex.Match(blobName.Replace(".json",""), "[\\.](.*)", RegexOptions.Singleline).Groups[1].Value);
            }

            public static string ToVersionKey(long version)
            {
                return version.ToString().PadLeft(10, '0');
            }
        }
        
        #endregion
    }
}