using System;

namespace CosmosDbMigrator
{
    public class SourceEvent
    {
        public Guid HolderId { get; set; }

        public Guid PlayerId { get; set; }

        public Guid ContentId { get; set; }

        public string Type { get; set; }

        public string IP { get; set; }

        public string Region { get; set; }

        public string Device { get; set; }

        public string Browser { get; set; }

        public string OperationSystem { get; set; }

        public int Bitrate { get; set; }

        public long timestamp { get; set; }

        public string Id { get; set; }
    }
}
