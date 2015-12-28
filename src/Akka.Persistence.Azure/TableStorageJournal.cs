using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Akka.Event;
using Akka.Persistence;
using Akka.Persistence.Azure;
using Akka.Persistence.Journal;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;

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
                IEnumerable<Event> results =  _extension.TableJournalSettings
                    .GetClient(persistenceId)
                    .GetTableReference(_extension.TableJournalSettings.TableName)
                    .ExecuteQuery(BuildTopVersionQuery(persistenceId, fromSequenceNr));
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
                    IEnumerable<Event> results = _extension.TableJournalSettings
                        .GetClient(persistenceId)
                        .GetTableReference(_extension.TableJournalSettings.TableName)
                        .ExecuteQuery(BuildReplayTableQuery(persistenceId,fromSequenceNr, toSequenceNr));

                    foreach (Event @event in results)
                    {
                        var actualEvent = JsonConvert.DeserializeObject(@event.EventState, _settings);
                        replayCallback(new Persistent(actualEvent, @event.SequenceNr, @event.Type, @event.AggregateId, @event.IsDeleted, Context.Self));
                        count++;
                        if (count == max) return;
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
                    TableBatchOperation batchOperation = new TableBatchOperation();
                    foreach(Event evt in events)
                    {
                        batchOperation.Insert(evt);
                    }

                    await _extension.TableJournalSettings
                        .GetClient(stream)
                        .GetTableReference(_extension.TableJournalSettings.TableName)
                        .ExecuteBatchAsync(batchOperation);
                }
                catch(Exception ex)
                {
                    _log.Error(ex, ex.Message);
                    throw;
                }
            }
        }

        protected override async Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr, bool isPermanent)
        {
            try
            {
                CloudTable table = _extension.TableJournalSettings
                    .GetClient(persistenceId)
                    .GetTableReference(_extension.TableJournalSettings.TableName);
                
                IEnumerable<Event> results = 
                    table.ExecuteQuery(BuildDeleteTableQuery(persistenceId, toSequenceNr))
                    .OrderByDescending(t => t.SequenceNr);
                if (results.Any())
                {
                    TableBatchOperation batchOperation = new TableBatchOperation();
                    foreach (Event s in results)
                    {
                        s.IsDeleted = true;
                        if(isPermanent)
                        {
                            batchOperation.Delete(s);
                        }
                        else
                        {
                            batchOperation.Replace(s);
                        }
                    }
                    await table.ExecuteBatchAsync(batchOperation);
                }
            }
            catch (Exception ex)
            {
                _log.Error(ex, ex.Message);
                throw;
            }
        }

        private static TableQuery<Event> BuildDeleteTableQuery(string persistenceId, long sequence)
        {
            return new TableQuery<Event>().Where(
                        TableQuery.CombineFilters(
                                TableQuery.GenerateFilterCondition("PartitionKey",
                                    QueryComparisons.Equal, persistenceId), TableOperators.And,
                                    TableQuery.GenerateFilterCondition("RowKey",
                                QueryComparisons.LessThanOrEqual, Event.ToRowKey(sequence)))
                            );
        }

        private static TableQuery<Event> BuildReplayTableQuery(string persistenceId, long fromSequenceNr, long toSequenceNr)
        {
            return new TableQuery<Event>().Where(
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
        }

        private static TableQuery<Event> BuildTopVersionQuery(string persistenceId, long fromSequenceNr)
        {
            return new TableQuery<Event>().Where(
                        TableQuery.CombineFilters(
                                TableQuery.GenerateFilterCondition("PartitionKey",
                                    QueryComparisons.Equal, persistenceId), TableOperators.And,
                                    TableQuery.GenerateFilterCondition("RowKey",
                                QueryComparisons.GreaterThanOrEqual, Event.ToRowKey(fromSequenceNr)))
                            );
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