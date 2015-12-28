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

namespace TableStorage.Persistence
{
    public class TableStorageSnapshotStore : SnapshotStore
    {
        private ILoggingAdapter _log;
        private JsonSerializerSettings _settings;
        private readonly AzureStoragePersistenceExtension _extension;

        public TableStorageSnapshotStore()
        {
            _extension = AzureStoragePersistence.Instance.Apply(Context.System);
            _log = Context.GetLogger();
            _settings = new JsonSerializerSettings();
            _settings.TypeNameHandling = TypeNameHandling.All;
        }
               
        protected override async Task<SelectedSnapshot> LoadAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            CloudTableClient tableClient = _extension.TableSnapshotStoreSettings.GetClient(persistenceId);
            CloudTable table = tableClient.GetTableReference(_extension.TableSnapshotStoreSettings.TableName);
            
            TableQuery<Snapshot> query =
                        new TableQuery<Snapshot>().Where(
                        TableQuery.CombineFilters(
                                TableQuery.GenerateFilterCondition("PartitionKey",
                                    QueryComparisons.Equal, persistenceId), TableOperators.And,
                                    TableQuery.GenerateFilterCondition("RowKey",
                                QueryComparisons.LessThanOrEqual, Snapshot.ToRowKey(criteria.MaxSequenceNr)))
                            );
            IEnumerable<Snapshot> results = table.ExecuteQuery(query).OrderByDescending(t=>t.SequenceNr);
            if (results.Count() > 0)
            {
                _log.Debug("Found snapshot of {0}", persistenceId);
                var result = results.First();
                
                var snapshot = JsonConvert.DeserializeObject(result.SnapshotState, typeof(SelectedSnapshot),_settings);
                if (((SelectedSnapshot)snapshot).Metadata.Timestamp > criteria.MaxTimeStamp)
                    return null;
                return (SelectedSnapshot)snapshot;
            }
            else return null;
        }

        protected override async Task SaveAsync(SnapshotMetadata metadata, object snapshot)
        {
            var snapData = new Snapshot(Guid.NewGuid(), metadata.PersistenceId, typeof(Snapshot).Name, metadata.SequenceNr, new SelectedSnapshot(metadata, snapshot));
            try
            {
                CloudTableClient tableClient = _extension.TableSnapshotStoreSettings.GetClient(metadata.PersistenceId);
                CloudTable table = tableClient.GetTableReference(_extension.TableSnapshotStoreSettings.TableName);
                TableOperation insertOperation = TableOperation.Insert(snapData);
                TableResult result = await table.ExecuteAsync(insertOperation);
            }
            catch (Exception ex)
            {
                _log.Error(ex,ex.Message);
            }
        }

        protected override void Saved(SnapshotMetadata metadata)
        {}

        protected override async Task DeleteAsync(SnapshotMetadata metadata)
        {
            try
            {
                CloudTableClient tableClient = _extension.TableSnapshotStoreSettings.GetClient(metadata.PersistenceId);
                CloudTable table = tableClient.GetTableReference(_extension.TableSnapshotStoreSettings.TableName);
                TableOperation getOperation = TableOperation.Retrieve<Snapshot>(metadata.PersistenceId, Snapshot.ToRowKey(metadata.SequenceNr));
                TableResult result = table.Execute(getOperation);
                TableOperation deleteOperation = TableOperation.Delete((Snapshot)result.Result);
                result = table.Execute(deleteOperation);
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
                CloudTableClient tableClient = _extension.TableSnapshotStoreSettings.GetClient(persistenceId);
                CloudTable table = tableClient.GetTableReference(_extension.TableSnapshotStoreSettings.TableName);
                TableQuery<Snapshot> query =
                        new TableQuery<Snapshot>().Where(
                        TableQuery.CombineFilters(
                                TableQuery.GenerateFilterCondition("PartitionKey",
                                    QueryComparisons.Equal, persistenceId), TableOperators.And,
                                    TableQuery.GenerateFilterCondition("RowKey",
                                QueryComparisons.LessThanOrEqual, Snapshot.ToRowKey(criteria.MaxSequenceNr)))
                            );
                IEnumerable<Snapshot> results = table.ExecuteQuery(query).OrderByDescending(t => t.SequenceNr);
                if (results.Count() > 0)
                {
                    TableBatchOperation batchOperation = new TableBatchOperation();
                    foreach (Snapshot s in results)
                    {
                            batchOperation.Delete(s);
                    }
                    table.ExecuteBatch(batchOperation);
                }
            }
            catch (Exception ex)
            {
                _log.Error(ex, ex.Message);
                throw;
            }
        }

        #region Internal Snapshot for Azure TableEntity

        private class Snapshot : TableEntity
        {
            public Guid Id { get; set; }
            public string AggregateId { get; set; }
            public string Type { get; set; }
            public long SequenceNr { get; set; }
            public string SnapshotState { get; set; }

            private JsonSerializerSettings _settings;

            public Snapshot()
            {
                _settings = new JsonSerializerSettings();
                _settings.TypeNameHandling = TypeNameHandling.All;
            }

            public Snapshot(Guid id, string aggId, string type, long version, object @event)
            {
                _settings = new JsonSerializerSettings();
                _settings.TypeNameHandling = TypeNameHandling.All;
                PartitionKey = aggId;
                RowKey = ToRowKey(version);
                Id = id;
                AggregateId = aggId;
                Type = type;
                SequenceNr = version;
                SnapshotState = JsonConvert.SerializeObject(@event, _settings);
            }

            public override string ToString()
            {
                return JsonConvert.SerializeObject(this, Formatting.Indented);
            }

            public static string ToRowKey(int version)
            {
                return version.ToString().PadLeft(10, '0');
            }

            public static string ToRowKey(long version)
            {
                return version.ToString().PadLeft(10, '0');
            }
        }
        #endregion
    }
}