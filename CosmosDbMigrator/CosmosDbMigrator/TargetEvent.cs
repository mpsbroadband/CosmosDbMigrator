using System;

namespace CosmosDbMigrator
{
    public class TargetEvent
    {
        public Guid holderId { get; set; }

        public Guid playerId { get; set; }

        public Guid contentId { get; set; }

        public string type { get; set; }

        public string ip { get; set; }

        public string region { get; set; }

        public string device { get; set; }

        public string browser { get; set; }

        public string operationSystem { get; set; }

        public int bitrate { get; set; }

        public long startTime { get; set; }
        public long endTime { get; set; }

        public string id { get; set; }
    }
}