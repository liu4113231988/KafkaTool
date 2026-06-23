using System;
using System.Collections.Generic;
using System.Linq;

namespace KafkaToolWpf.Models
{
    public class TopicInfo
    {
        public string TopicName { get; set; }
        public int PartitionCount { get; set; }
        public short ReplicationFactor { get; set; }
        public List<PartitionInfo> Partitions { get; set; } = new();
        public Dictionary<string, string> Configs { get; set; } = new();
        public int UnderReplicatedPartitionCount => Partitions.Count(p => p.IsUnderReplicated);
        public int OfflinePartitionCount => Partitions.Count(p => !p.HasLeader);
        public long TotalMessageCount => Partitions.Sum(p => p.MessageCount);
        public string PartitionRiskSummary => $"副本未同步 {UnderReplicatedPartitionCount} 个；无 Leader {OfflinePartitionCount} 个";
        public string HealthSummary =>
            OfflinePartitionCount > 0
                ? $"异常: {OfflinePartitionCount} 个分区无 Leader"
                : UnderReplicatedPartitionCount > 0
                    ? $"注意: {UnderReplicatedPartitionCount} 个分区副本未同步"
                    : "健康";
    }

    public class PartitionInfo
    {
        public int PartitionId { get; set; }
        public int Leader { get; set; }
        public List<int> Replicas { get; set; } = new();
        public List<int> InSyncReplicas { get; set; } = new();
        public long? EarliestOffset { get; set; }
        public long? LatestOffset { get; set; }
        public long MessageCount => (LatestOffset ?? 0) - (EarliestOffset ?? 0);
        public string ReplicasText => string.Join(", ", Replicas);
        public string InSyncReplicasText => string.Join(", ", InSyncReplicas);
        public bool HasLeader => Leader >= 0;
        public bool IsUnderReplicated => Replicas.Count != InSyncReplicas.Count;
        public string StatusText =>
            !HasLeader ? "Leader 异常" :
            IsUnderReplicated ? "副本未同步" :
            "正常";
    }

    public class BrokerInfo
    {
        public int BrokerId { get; set; }
        public string Host { get; set; }
        public int Port { get; set; }
        public string Endpoint => $"{Host}:{Port}";
    }

    public class ClusterInfo
    {
        public string ClusterId { get; set; }
        public int ControllerId { get; set; }
        public List<BrokerInfo> Brokers { get; set; } = new();
        public int TopicCount { get; set; }
        public int BrokerCount => Brokers.Count;
    }

    public class ConsumerGroupInfo
    {
        public string GroupId { get; set; }
        public string State { get; set; } // Stable, Empty, Dead, etc.
        public string ProtocolType { get; set; }
        public List<MemberInfo> Members { get; set; } = new();
        public int MemberCount => Members.Count;
    }

    public class MemberInfo
    {
        public string MemberId { get; set; }
        public string ClientId { get; set; }
        public string Host { get; set; }
        public List<string> AssignedPartitions { get; set; } = new();
        public string AssignedPartitionsText => string.Join(", ", AssignedPartitions);
    }

    public class ConsumerGroupDetail
    {
        public string GroupId { get; set; }
        public string Topic { get; set; }
        public int Partition { get; set; }
        public long CurrentOffset { get; set; }
        public long LogEndOffset { get; set; }
        public long Lag => LogEndOffset - CurrentOffset;
        public string ClientId { get; set; }
        public string Host { get; set; }
        public string TopicPartitionText => $"{Topic}[{Partition}]";
        public string OwnerText => string.IsNullOrWhiteSpace(ClientId) ? Host : $"{ClientId} @ {Host}";
    }

    public class MessageRecord
    {
        public string Topic { get; set; }
        public int Partition { get; set; }
        public long Offset { get; set; }
        public string Key { get; set; }
        public string Value { get; set; }
        public long Timestamp { get; set; }
        public string TimestampType { get; set; }
        public Dictionary<string, string> Headers { get; set; } = new();
        public string FormattedTimestamp =>
            DateTimeOffset.FromUnixTimeMilliseconds(Timestamp).LocalDateTime.ToString("yyyy-MM-dd HH:mm:ss.fff");
        public string FormattedHeaders =>
            Headers == null || Headers.Count == 0
                ? string.Empty
                : string.Join("; ", Headers.Select(h => $"{h.Key}={h.Value}"));
    }

    public class ConnectionConfig
    {
        public string Name { get; set; }
        public string BootstrapServers { get; set; }
        public bool UseSasl { get; set; }
        public string SaslMechanism { get; set; } = "PLAIN";
        public string SaslUsername { get; set; }
        public string SaslPassword { get; set; }
        public bool UseSsl { get; set; }
        public string SslCaLocation { get; set; }
        public string SslCertificateLocation { get; set; }
        public string SslKeyLocation { get; set; }
        public bool SslSkipVerify { get; set; }
    }
}
