using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Runtime.Serialization.Formatters;
using System.Text;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Event;
using Akka.Persistence;
using Akka.Persistence.Journal;
using Akka.Serialization;
using Newtonsoft.Json;
using System.Configuration;
using System.Threading;
using Microsoft.WindowsAzure.Storage.Table;
using Akka.Persistence.Azure;

namespace TableStorage.Persistence
{
    public class TableStorageJournal : AsyncWriteJournal
    {
        private ILoggingAdapter _log;
        private JsonSerializerSettings _settings;
        private readonly AzureStoragePersistenceExtension _extension;

        public TableStorageJournal()
        {
            _log = Context.GetLogger();
            _extension = AzureStoragePersistence.Instance.Apply(Context.System);
            _settings = new JsonSerializerSettings();
            _settings.TypeNameHandling = TypeNameHandling.All;
        }

        public override async Task<long> ReadHighestSequenceNrAsync(string persistenceId, long fromSequenceNr)
        {
            try
            {
                CloudTableClient tableClient = _extension.TableJournalSettings.GetClient(persistenceId);
                CloudTable table = tableClient.GetTableReference(_extension.TableJournalSettings.TableName);
                TableQuery<Event> query =
                        new TableQuery<Event>().Where(
                        TableQuery.CombineFilters(
                                TableQuery.GenerateFilterCondition("PartitionKey",
                                    QueryComparisons.Equal, persistenceId), TableOperators.And, 
                                    TableQuery.GenerateFilterCondition("RowKey",
                                QueryComparisons.GreaterThanOrEqual, Event.ToRowKey(fromSequenceNr)))
                            );
                IEnumerable<Event> results = table.ExecuteQuery(query);
                
                if (results.Any())
                {
                    var highest = results.Max(t => t.SequenceNr);
                    return highest;
                }
                else return 0;
            }
            catch (Exception e)
            {
                _log.Error(e, e.Message);
                throw;
            }
        }

        public override async Task ReplayMessagesAsync(string persistenceId, long fromSequenceNr, long toSequenceNr, long max, Action<IPersistentRepresentation> replayCallback)
        {
            try
            {
                long count = 0;
                if (max > 0 && (toSequenceNr - fromSequenceNr) >= 0)
                {
                    CloudTableClient tableClient = _extension.TableJournalSettings.GetClient(persistenceId);
                    CloudTable table = tableClient.GetTableReference(_extension.TableJournalSettings.TableName);
                    TableQuery<Event> query =
                            new TableQuery<Event>().Where(
                                TableQuery.CombineFilters(
                                    TableQuery.CombineFilters(
                                            TableQuery.GenerateFilterCondition("PartitionKey",
                                                QueryComparisons.Equal, persistenceId), TableOperators.And,
                                                TableQuery.GenerateFilterCondition("RowKey",
                                            QueryComparisons.GreaterThanOrEqual, Event.ToRowKey(fromSequenceNr))),
                                    TableOperators.And,
                                        TableQuery.GenerateFilterCondition("RowKey",
                                            QueryComparisons.LessThanOrEqual, Event.ToRowKey(toSequenceNr))
                                        ));
                    
                    IEnumerable<Event> results = table.ExecuteQuery(query);

                    foreach (Event @event in results)
                    {
                        if (!@event.IsPermanentDelete)
                        {
                            var actualEvent = JsonConvert.DeserializeObject(@event.EventState, _settings);
                            replayCallback(new Persistent(actualEvent, @event.SequenceNr, @event.AggregateId, @event.IsDeleted, Context.Self));
                            count++;
                            if (count == max) return;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                _log.Error(e, e.Message);
                throw;
            }
        }

        protected override async Task WriteMessagesAsync(IEnumerable<IPersistentRepresentation> messages)
        {
            
            foreach (var grouping in messages.GroupBy(x => x.PersistenceId))
            {
                var stream = grouping.Key;

                var representations = grouping.OrderBy(x => x.SequenceNr).ToArray();

                var events = representations.Select(x =>
                {
                    var eventId = GuidUtility.Create(GuidUtility.IsoOidNamespace, string.Concat(stream, x.SequenceNr));
                    return new Event(eventId, x.PersistenceId, x.Payload.GetType().FullName, x.SequenceNr, x.Payload);
                });
                try
                {
                    CloudTableClient tableClient = _extension.TableJournalSettings.GetClient(stream);
                    CloudTable table = tableClient.GetTableReference(_extension.TableJournalSettings.TableName);
                    TableBatchOperation batchOperation = new TableBatchOperation();
                    foreach(Event evt in events)
                    {
                        batchOperation.Insert(evt);
                    }
                    
                    IList<TableResult> results = await table.ExecuteBatchAsync(batchOperation);
                }
                catch(Exception ex)
                {
                    _log.Error(ex, ex.Message);
                    throw;
                }
            }
        }

        protected override Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr, bool isPermanent)
        {
            try
            {
                CloudTableClient tableClient = _extension.TableJournalSettings.GetClient(persistenceId);
                CloudTable table = tableClient.GetTableReference(_extension.TableJournalSettings.TableName);
                TableQuery<Event> query =
                        new TableQuery<Event>().Where(
                        TableQuery.CombineFilters(
                                TableQuery.GenerateFilterCondition("PartitionKey",
                                    QueryComparisons.Equal, persistenceId), TableOperators.And,
                                    TableQuery.GenerateFilterCondition("RowKey",
                                QueryComparisons.LessThanOrEqual, Event.ToRowKey(toSequenceNr)))
                            );
                IEnumerable<Event> results = table.ExecuteQuery(query).OrderByDescending(t => t.SequenceNr);
                if (results.Count() > 0)
                {
                    TableBatchOperation batchOperation = new TableBatchOperation();
                    foreach (Event s in results)
                    {
                        s.IsDeleted = true;
                        s.IsPermanentDelete = isPermanent;
                        batchOperation.Replace(s);
                    }
                    table.ExecuteBatch(batchOperation);
                }
                return Task.FromResult<object>(null);
            }
            catch (Exception ex)
            {
                _log.Error(ex, ex.Message);
                throw;
            }
        }

        #region Internal Event class for Azure TableEntity

        private class Event : TableEntity
        {
            public Guid Id { get; set; }
            public string AggregateId { get; set; }
            public string Type { get; set; }
            public long SequenceNr { get; set; }
            public string EventState { get; set; }
            public bool IsDeleted { get; set; }
            public bool IsPermanentDelete { get; set; }

            private JsonSerializerSettings _settings;

            public Event()
            {
                _settings = new JsonSerializerSettings();
                _settings.TypeNameHandling = TypeNameHandling.All;
            }

            public Event(Guid id, string aggId, string type, long version, object @event)
            {
                _settings = new JsonSerializerSettings();
                _settings.TypeNameHandling = TypeNameHandling.All;
                PartitionKey = aggId;
                RowKey = ToRowKey(version);
                Id = id;
                AggregateId = aggId;
                Type = type;
                SequenceNr = version;
                EventState = JsonConvert.SerializeObject(@event, _settings);
            }

            public override string ToString()
            {
                return JsonConvert.SerializeObject(this, Formatting.Indented);
            }

            public static string ToRowKey(long version)
            {
                return version.ToString().PadLeft(10, '0');
            }
        }

        #endregion
    }

    
}