using Microsoft.Azure.Documents.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Linq;
using NodaTime;

namespace CosmosDbMigrator
{
    class Program
    {
        private const string SourceUrl = "";
        private const string TargetUrl = "";

        private const string SourceAuthKey =
            "zWLazWb9UibqZ5o8FGd242UypdreQrRAnR0JlBCXA65X82xljZ6ZnGQUcAp7nvt5nzpJ6YFmANPnl9juIXN6IA==";

        private const string TargetAuthKey =
            "zWLazWb9UibqZ5o8FGd242UypdreQrRAnR0JlBCXA65X82xljZ6ZnGQUcAp7nvt5nzpJ6YFmANPnl9juIXN6IA==";

        private const string CollectionName = "Order";

        private const string SourceDatabaseName = "Statistics";
        private const string TargetDatabaseName = "Statistic";

        private const bool Transform = false;

        private const int Skip = 0;

        private static List<TargetEvent> _cache;

        private static void Main(string[] args)
        {
            _cache = new List<TargetEvent>();

            var sourceClient = new DocumentClient(new Uri(SourceUrl), SourceAuthKey,
                new ConnectionPolicy
                {
                    MaxConnectionLimit = 1000,
                    ConnectionMode = ConnectionMode.Direct,
                    ConnectionProtocol = Protocol.Tcp
                });

            var targetClient = (SourceUrl == TargetUrl)
                ? sourceClient
                : new DocumentClient(new Uri(TargetUrl), TargetAuthKey,
                    new ConnectionPolicy
                    {
                        MaxConnectionLimit = 1000,
                        ConnectionMode = ConnectionMode.Direct,
                        ConnectionProtocol = Protocol.Tcp
                    });

            if (Transform)
            {
                var query =
                    sourceClient.CreateDocumentQuery<SourceEvent>(
                        UriFactory.CreateDocumentCollectionUri(SourceDatabaseName, CollectionName),
                        new FeedOptions
                        {
                            MaxDegreeOfParallelism = -1,
                            EnableCrossPartitionQuery = true
                        }).OrderBy(e => e.timestamp).AsDocumentQuery();

                var count = 0;
                while (query.HasMoreResults)
                {
                    var result = query.ExecuteNextAsync<SourceEvent>().Result;
                    var tasks = new List<Task>();

                    foreach (var sourceEvent in result)
                    {
                        if (count >= Skip)
                        {
                            var targetEvent = CreateEvent(sourceEvent);

                            tasks.Add(targetClient.UpsertDocumentAsync(
                                UriFactory.CreateDocumentCollectionUri(TargetDatabaseName, CollectionName),
                                targetEvent));
                        }

                        else
                        {
                            tasks.Add(Task.Run(() => { }));
                        }
                    }

                    count += tasks.Count;
                    Task.WhenAll(tasks).Wait();
                    var percentage = (int)Math.Round(_cache.Count / (double)count * 100);
                    Console.WriteLine("Read: {0}. Written: {1}. ({2}%)", count, _cache.Count, percentage);
                }
            }
            else
            {
                var query =
                    sourceClient.CreateDocumentQuery<Document>(
                        UriFactory.CreateDocumentCollectionUri(SourceDatabaseName, CollectionName),
                        new FeedOptions
                        {
                            MaxDegreeOfParallelism = -1,
                            EnableCrossPartitionQuery = true
                        }).OrderBy(d => d.Timestamp).AsDocumentQuery();

                var count = 0;
                while (query.HasMoreResults)
                {
                    var result = query.ExecuteNextAsync<Document>().Result;
                    var tasks = new List<Task>();

                    foreach (var document in result)
                    {
                        if (count >= Skip)
                        {
                            tasks.Add(targetClient.UpsertDocumentAsync(
                                UriFactory.CreateDocumentCollectionUri(TargetDatabaseName, CollectionName), document));
                        }
                        else
                        {
                            tasks.Add(Task.Run(() => { }));
                        }
                    }

                    count += tasks.Count;
                    Task.WhenAll(tasks).Wait();
                    Console.WriteLine("Copied {0} documents.", count);
                }
            }
        }

        private static TargetEvent CreateEvent(SourceEvent sourceEvent)
        {
            var existingEvent = _cache.SingleOrDefault(ev =>
                ev.playerId == sourceEvent.PlayerId &&
                ev.contentId == sourceEvent.ContentId &&
                ev.type == sourceEvent.Type &&
                ev.ip == sourceEvent.IP &&
                ev.device == sourceEvent.Device &&
                ev.browser == sourceEvent.Browser &&
                ev.operationSystem == sourceEvent.OperationSystem &&
                ev.bitrate == sourceEvent.Bitrate &&
                sourceEvent.timestamp - ev.endTime < Duration.FromMinutes(1).TotalTicks
                );

            if (existingEvent == null)
            {
                var newEvent = new TargetEvent
                {
                    bitrate = sourceEvent.Bitrate,
                    browser = sourceEvent.Browser,
                    contentId = sourceEvent.ContentId,
                    device = sourceEvent.Device,
                    endTime = sourceEvent.timestamp,
                    holderId = sourceEvent.HolderId,
                    id = sourceEvent.Id,
                    ip = sourceEvent.IP,
                    operationSystem = sourceEvent.OperationSystem,
                    playerId = sourceEvent.PlayerId,
                    region = sourceEvent.Region,
                    startTime = sourceEvent.timestamp,
                    type = sourceEvent.Type
                };
                _cache.Add(newEvent);
                return newEvent;
            }

            if(sourceEvent.timestamp > existingEvent.endTime)
                existingEvent.endTime = sourceEvent.timestamp;
            return existingEvent;
        }
    }
}