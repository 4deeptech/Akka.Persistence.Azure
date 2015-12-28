using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Akka;
using Akka.Actor;
using Akka.Event;
using Akka.Persistence;
using Akka.Persistence.Azure;
using Akka.Persistence.Snapshot;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;

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
                            var snapshot = JsonConvert.DeserializeObject(Encoding.UTF8.GetString(memoryStream.ToArray()), 
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
                using (var memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(snap)))
                {
                    memoryStream.Position = 0;
                    blockBlob.Metadata.Add("SnapshotTimestamp", metadata.Timestamp.Ticks.ToString());
                    blockBlob.Metadata.Add("Version", SnapshotVersionHelper.ToVersionKey(metadata.SequenceNr));
                    await blockBlob.UploadFromStreamAsync(memoryStream);
                }
            }
            catch (Exception ex)
            {
                _log.Error(ex, ex.Message);
            }
        }

        protected override void Saved(SnapshotMetadata metadata)
        {}

        protected override async Task DeleteAsync(SnapshotMetadata metadata)
        {
            try
            {
                IEnumerable<CloudBlockBlob> blobs = _extension
                .BlobSnapshotStoreSettings
                .GetBlobClient(metadata.PersistenceId)
                .GetContainerReference(_extension.BlobSnapshotStoreSettings.ContainerName)
                .ListBlobs(metadata.PersistenceId + "." + SnapshotVersionHelper.ToVersionKey(metadata.SequenceNr) + ".json", true, BlobListingDetails.All).Cast<CloudBlockBlob>();

                var deleteTasks = blobs.Select(clob =>
                {
                    _log.Debug("Found snapshot to delete for {0}", metadata.PersistenceId);
                    return clob.DeleteIfExistsAsync();
                });

                await Task.WhenAll(deleteTasks);
            }
            catch (Exception ex)
            {
                _log.Error(ex, ex.Message);
            }
        }

        protected override async Task DeleteAsync(string persistenceId, SnapshotSelectionCriteria criteria)
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
                    var deleteTasks = TryDeleteAsync(persistenceId, criteria, results);
                    await Task.WhenAll(deleteTasks);
                }
            }
            catch (Exception ex)
            {
                _log.Error(ex, ex.Message);
                throw;
            }
        }

        private IEnumerable<Task> TryDeleteAsync(string persistenceId, SnapshotSelectionCriteria criteria, IOrderedEnumerable<CloudBlockBlob> results)
        {
            foreach (CloudBlockBlob clob in results)
            {
                if (SnapshotVersionHelper.Parse(clob.Name) <= criteria.MaxSequenceNr
                    && long.Parse(clob.Metadata["SnapshotTimestamp"]) <= criteria.MaxTimeStamp.Ticks)
                {
                    _log.Debug("Found snapshot to remove for {0}", persistenceId);
                    yield return clob.DeleteIfExistsAsync();
                }
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