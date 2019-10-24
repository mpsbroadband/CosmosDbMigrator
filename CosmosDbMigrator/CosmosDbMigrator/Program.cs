using Microsoft.Azure.Documents.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.Linq;
using Newtonsoft.Json;
using NodaTime;

namespace CosmosDbMigrator
{
    class Program
    {
        private const string SourceUrl = "https://livearenastage.documents.azure.com:443/";
        private const string TargetUrl = "https://livearenastage.documents.azure.com:443/";

        private const string SourceAuthKey =
            "Y4KrcfgA4E2f4G40Cow4JOQEqACQSzl3w8SRklJVS1wwHYiRUOCjgfWoQLuSBYoKvO5kNl9LF0K88XXx6wTuqg==";

        private const string TargetAuthKey =
            "Y4KrcfgA4E2f4G40Cow4JOQEqACQSzl3w8SRklJVS1wwHYiRUOCjgfWoQLuSBYoKvO5kNl9LF0K88XXx6wTuqg==";

        private const string CollectionName = "Event";

        private const string SourceDatabaseName = "Statistics";
        private const string TargetDatabaseName = "Statistics4";

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

            var query =
                sourceClient.CreateDocumentQuery<SourceEvent>(
                    UriFactory.CreateDocumentCollectionUri(SourceDatabaseName, CollectionName),
                    new FeedOptions
                    {
                        MaxDegreeOfParallelism = -1,
                        EnableCrossPartitionQuery = true
                    }).OrderBy(e => e.timestamp).AsDocumentQuery();

            var dict = JsonConvert.DeserializeObject<Dictionary<string, string>>(query.ToString());
            var queryString = dict["query"];

            var count = 0;
            while (query.HasMoreResults)
            {
                var result = query.ExecuteNextAsync<SourceEvent>().Result;
                var tasks = new List<Task>();

                foreach (var sourceEvent in result)
                {
                    var targetEvent = CreateEvent(sourceEvent);

                    tasks.Add(targetClient.UpsertDocumentAsync(
                        UriFactory.CreateDocumentCollectionUri(TargetDatabaseName, CollectionName), targetEvent));
                }

                count += tasks.Count;
                Task.WhenAll(tasks).Wait();
                var percentage = (int)Math.Round(_cache.Count / (double) count * 100);
                Console.WriteLine("Read: {0}. Written: {1}. ({2}%)", count, _cache.Count, percentage);
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