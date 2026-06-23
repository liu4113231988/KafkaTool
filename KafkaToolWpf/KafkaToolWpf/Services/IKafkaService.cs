using KafkaToolWpf.Models;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace KafkaToolWpf.Services
{
    public interface IKafkaService
    {
        Task<ClusterInfo> GetClusterInfoAsync(string bootstrapServers, Action<Confluent.Kafka.AdminClientConfig> configAction = null);
        Task<List<TopicInfo>> GetTopicListAsync(string bootstrapServers, Action<Confluent.Kafka.AdminClientConfig> configAction = null);
        Task<TopicInfo> GetTopicDetailAsync(string bootstrapServers, string topicName, Action<Confluent.Kafka.AdminClientConfig> configAction = null);
        Task<bool> CreateTopicAsync(string bootstrapServers, string topicName, int numPartitions, short replicationFactor, Dictionary<string, string> configs = null, Action<Confluent.Kafka.AdminClientConfig> configAction = null);
        Task<bool> DeleteTopicAsync(string bootstrapServers, string topicName, Action<Confluent.Kafka.AdminClientConfig> configAction = null);
        Task<List<ConsumerGroupInfo>> GetConsumerGroupsAsync(string bootstrapServers, Action<Confluent.Kafka.AdminClientConfig> configAction = null);
        Task<List<ConsumerGroupDetail>> GetConsumerGroupDetailsAsync(string bootstrapServers, string groupId, Action<Confluent.Kafka.AdminClientConfig> configAction = null);
        Task<bool> DeleteConsumerGroupAsync(string bootstrapServers, string groupId, Action<Confluent.Kafka.AdminClientConfig> configAction = null);
        Task<List<MessageRecord>> BrowseMessagesAsync(string bootstrapServers, string topic, int? partition, long? offset, int count, string groupId = null, Action<Confluent.Kafka.ConsumerConfig> configAction = null);
        Task<List<MessageRecord>> ConsumeFromTimestampAsync(string bootstrapServers, string topic, DateTime timestamp, int count, string groupId = null, Action<Confluent.Kafka.ConsumerConfig> configAction = null);
        Task<bool> SendMessageAsync(string bootstrapServers, string topic, string key, string value, int? partition = null, Dictionary<string, string> headers = null, Action<Confluent.Kafka.ProducerConfig> configAction = null);
        Task<bool> TestConnectionAsync(string bootstrapServers, Action<Confluent.Kafka.AdminClientConfig> configAction = null);
        Task<bool> ResetConsumerGroupOffsetAsync(string bootstrapServers, string groupId, string topic, long? offset, Action<Confluent.Kafka.AdminClientConfig> configAction = null);
    }
}