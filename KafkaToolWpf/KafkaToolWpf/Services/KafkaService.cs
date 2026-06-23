using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaToolWpf.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace KafkaToolWpf.Services
{
    public class KafkaService : IKafkaService
    {
        private AdminClientConfig BuildAdminConfig(string bootstrapServers, Action<AdminClientConfig> configAction = null)
        {
            var config = new AdminClientConfig { BootstrapServers = bootstrapServers };
            configAction?.Invoke(config);
            return config;
        }

        private ProducerConfig BuildProducerConfig(string bootstrapServers, Action<ProducerConfig> configAction = null)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                MessageTimeoutMs = 10000
            };
            configAction?.Invoke(config);
            return config;
        }

        private ConsumerConfig BuildConsumerConfig(string bootstrapServers, string groupId, Action<ConsumerConfig> configAction = null)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = groupId ?? $"kafka-tool-{Guid.NewGuid():N}",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true,
                SessionTimeoutMs = 6000,
            };
            configAction?.Invoke(config);
            return config;
        }

        public Task<ClusterInfo> GetClusterInfoAsync(string bootstrapServers, Action<AdminClientConfig> configAction = null)
        {
            var config = BuildAdminConfig(bootstrapServers, configAction);
            using var adminClient = new AdminClientBuilder(config).Build();
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));

            var cluster = new ClusterInfo
            {
                ClusterId = metadata.OriginatingBrokerName,
                ControllerId = metadata.OriginatingBrokerId,
                Brokers = metadata.Brokers.Select(b => new BrokerInfo
                {
                    BrokerId = b.BrokerId,
                    Host = b.Host,
                    Port = b.Port
                }).ToList(),
                TopicCount = metadata.Topics.Count
            };

            return Task.FromResult(cluster);
        }

        public Task<List<TopicInfo>> GetTopicListAsync(string bootstrapServers, Action<AdminClientConfig> configAction = null)
        {
            var config = BuildAdminConfig(bootstrapServers, configAction);
            using var adminClient = new AdminClientBuilder(config).Build();
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));

            var topics = metadata.Topics
                .Where(t => t.Error.Code == ErrorCode.NoError && !t.Topic.StartsWith("__"))
                .Select(t =>
                {
                    var partitions = t.Partitions.Select(p => new Models.PartitionInfo
                    {
                        PartitionId = p.PartitionId,
                        Leader = p.Leader,
                        Replicas = p.Replicas.ToList(),
                        InSyncReplicas = p.InSyncReplicas.ToList()
                    }).ToList();

                    return new TopicInfo
                    {
                        TopicName = t.Topic,
                        PartitionCount = partitions.Count,
                        ReplicationFactor = partitions.Count > 0 ? (short)partitions[0].Replicas.Count : (short)0,
                        Partitions = partitions
                    };
                }).ToList();

            return Task.FromResult(topics);
        }

        public async Task<TopicInfo> GetTopicDetailAsync(string bootstrapServers, string topicName, Action<AdminClientConfig> configAction = null)
        {
            var config = BuildAdminConfig(bootstrapServers, configAction);
            using var adminClient = new AdminClientBuilder(config).Build();

            var metadata = adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(10));
            var topic = metadata.Topics.FirstOrDefault(t => t.Topic == topicName);
            if (topic == null)
                throw new Exception($"Topic '{topicName}' not found.");

            // Get topic configs - skip config describe if not supported
            var configDict = new Dictionary<string, string>();
            try
            {
                var configResources = new ConfigResource[] 
                { 
                    new ConfigResource { Type = ResourceType.Topic, Name = topicName } 
                };
                var configResult = await adminClient.DescribeConfigsAsync(configResources, null);
                // configResult is a collection of DescribeConfigsResult
                foreach (var result in configResult)
                {
                    foreach (var entry in result.Entries)
                    {
                        configDict[entry.Key] = entry.Value.Value;
                    }
                }
            }
            catch { /* Config API may not be available */ }

            // Get watermark offsets using a consumer
            using var consumer = new ConsumerBuilder<Ignore, Ignore>(new ConsumerConfig
            {
                BootstrapServers = bootstrapServers
            }).Build();

            var partitions = topic.Partitions.Select(p =>
            {
                long? earliest = null, latest = null;
                try
                {
                    var tp = new TopicPartition(topicName, p.PartitionId);
                    var watermarks = consumer.GetWatermarkOffsets(tp);
                    earliest = watermarks.Low;
                    latest = watermarks.High;
                }
                catch { }

                return new Models.PartitionInfo
                {
                    PartitionId = p.PartitionId,
                    Leader = p.Leader,
                    Replicas = p.Replicas.ToList(),
                    InSyncReplicas = p.InSyncReplicas.ToList(),
                    EarliestOffset = earliest,
                    LatestOffset = latest
                };
            }).ToList();

            consumer.Close();

            return new TopicInfo
            {
                TopicName = topicName,
                PartitionCount = partitions.Count,
                ReplicationFactor = partitions.Count > 0 ? (short)partitions[0].Replicas.Count : (short)0,
                Partitions = partitions,
                Configs = configDict
            };
        }

        public async Task<bool> CreateTopicAsync(string bootstrapServers, string topicName, int numPartitions,
            short replicationFactor, Dictionary<string, string> configs = null, Action<AdminClientConfig> configAction = null)
        {
            var config = BuildAdminConfig(bootstrapServers, configAction);
            using var adminClient = new AdminClientBuilder(config).Build();

            var topicConfig = new Dictionary<string, string>();
            if (configs != null)
            {
                foreach (var kv in configs)
                    topicConfig[kv.Key] = kv.Value;
            }

            var topicSpec = new TopicSpecification
            {
                Name = topicName,
                NumPartitions = numPartitions,
                ReplicationFactor = replicationFactor,
                Configs = topicConfig
            };

            await adminClient.CreateTopicsAsync(new[] { topicSpec }, null);
            return true;
        }

        public async Task<bool> DeleteTopicAsync(string bootstrapServers, string topicName, Action<AdminClientConfig> configAction = null)
        {
            var config = BuildAdminConfig(bootstrapServers, configAction);
            using var adminClient = new AdminClientBuilder(config).Build();

            await adminClient.DeleteTopicsAsync(new[] { topicName }, null);
            return true;
        }

        public Task<List<ConsumerGroupInfo>> GetConsumerGroupsAsync(string bootstrapServers, Action<AdminClientConfig> configAction = null)
        {
            var config = BuildAdminConfig(bootstrapServers, configAction);
            using var adminClient = new AdminClientBuilder(config).Build();

            var groups = adminClient.ListGroups(TimeSpan.FromSeconds(10));
            var result = groups.Select(g =>
            {
                var members = g.Members?.Select(m => new MemberInfo
                {
                    MemberId = m.MemberId,
                    ClientId = m.ClientId,
                    Host = m.ClientHost,
                    AssignedPartitions = new List<string> { $"{g.Group}" }
                }).ToList() ?? new List<MemberInfo>();

                return new ConsumerGroupInfo
                {
                    GroupId = g.Group,
                    State = g.State,
                    ProtocolType = g.ProtocolType,
                    Members = members
                };
            }).ToList();

            return Task.FromResult(result);
        }

        public Task<List<ConsumerGroupDetail>> GetConsumerGroupDetailsAsync(string bootstrapServers, string groupId, Action<AdminClientConfig> configAction = null)
        {
            var config = BuildAdminConfig(bootstrapServers, configAction);
            using var adminClient = new AdminClientBuilder(config).Build();

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = groupId,
                EnableAutoCommit = false,
                AutoOffsetReset = AutoOffsetReset.Latest
            };

            using var consumer = new ConsumerBuilder<Ignore, Ignore>(consumerConfig).Build();

            // Get metadata to list all topics and partitions
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
            var details = new List<ConsumerGroupDetail>();

            foreach (var topic in metadata.Topics.Where(t => !t.Topic.StartsWith("__")))
            {
                var topicPartitions = topic.Partitions.Select(p =>
                    new TopicPartition(topic.Topic, p.PartitionId)).ToList();

                try
                {
                    var committed = consumer.Committed(topicPartitions, TimeSpan.FromSeconds(10));
                    foreach (var cp in committed)
                    {
                        if (cp.Offset.Value == Offset.Unset) continue;

                        try
                        {
                            var watermarks = consumer.GetWatermarkOffsets(cp.TopicPartition);
                            details.Add(new ConsumerGroupDetail
                            {
                                GroupId = groupId,
                                Topic = cp.TopicPartition.Topic,
                                Partition = cp.TopicPartition.Partition,
                                CurrentOffset = cp.Offset.Value,
                                LogEndOffset = watermarks.High,
                                ClientId = "",
                                Host = ""
                            });
                        }
                        catch { }
                    }
                }
                catch { }
            }

            consumer.Close();
            return Task.FromResult(details);
        }

        public async Task<bool> DeleteConsumerGroupAsync(string bootstrapServers, string groupId, Action<AdminClientConfig> configAction = null)
        {
            var config = BuildAdminConfig(bootstrapServers, configAction);
            using var adminClient = new AdminClientBuilder(config).Build();

            await adminClient.DeleteGroupsAsync(new[] { groupId }, null);
            return true;
        }

        public Task<List<MessageRecord>> BrowseMessagesAsync(string bootstrapServers, string topic,
            int? partition, long? offset, int count, string groupId = null, Action<ConsumerConfig> configAction = null)
        {
            var consumerGroup = groupId ?? $"kafka-browse-{Guid.NewGuid():N}";
            var config = BuildConsumerConfig(bootstrapServers, consumerGroup, configAction);
            config.EnableAutoCommit = false;
            config.AutoOffsetReset = AutoOffsetReset.Earliest;

            using var consumer = new ConsumerBuilder<string, string>(config).Build();
            var messages = new List<MessageRecord>();

            if (partition.HasValue)
            {
                var tp = new TopicPartition(topic, partition.Value);
                consumer.Assign(tp);
                if (offset.HasValue)
                    consumer.Seek(new TopicPartitionOffset(tp, offset.Value));

                for (int i = 0; i < count; i++)
                {
                    try
                    {
                        var result = consumer.Consume(TimeSpan.FromSeconds(2));
                        if (result == null || result.IsPartitionEOF) break;

                        messages.Add(ToMessageRecord(result));
                    }
                    catch (ConsumeException) { break; }
                }
            }
            else
            {
                consumer.Subscribe(topic);
                for (int i = 0; i < count; i++)
                {
                    try
                    {
                        var result = consumer.Consume(TimeSpan.FromSeconds(2));
                        if (result == null || result.IsPartitionEOF) break;

                        messages.Add(ToMessageRecord(result));
                    }
                    catch (ConsumeException) { break; }
                }
            }

            consumer.Close();
            return Task.FromResult(messages);
        }

        public Task<List<MessageRecord>> ConsumeFromTimestampAsync(string bootstrapServers, string topic,
            DateTime timestamp, int count, string groupId = null, Action<ConsumerConfig> configAction = null)
        {
            var consumerGroup = groupId ?? $"kafka-ts-{Guid.NewGuid():N}";
            var config = BuildConsumerConfig(bootstrapServers, consumerGroup, configAction);
            config.EnableAutoCommit = false;
            config.AutoOffsetReset = AutoOffsetReset.Earliest;

            using var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();
            var metadata = adminClient.GetMetadata(topic, TimeSpan.FromSeconds(10));
            var topicMeta = metadata.Topics.FirstOrDefault(t => t.Topic == topic);
            if (topicMeta == null) return Task.FromResult(new List<MessageRecord>());

            var messages = new List<MessageRecord>();
            using var consumer = new ConsumerBuilder<string, string>(config).Build();

            foreach (var p in topicMeta.Partitions)
            {
                var tp = new TopicPartition(topic, p.PartitionId);
                var tsOffset = new Timestamp(timestamp);
                // OffsetsForTimes may not exist in all Consumer versions; use seeking by timestamp offset
                try
                {
                    var offsets = consumer.OffsetsForTimes(
                        new[] { new TopicPartitionTimestamp(tp, tsOffset) },
                        TimeSpan.FromSeconds(10));
                    if (offsets.Count > 0 && offsets[0].Offset != Offset.Unset)
                    {
                        consumer.Assign(new TopicPartitionOffset(offsets[0].TopicPartition, offsets[0].Offset));
                    }
                }
                catch
                {
                    // Skip partitions that can't be offset-seeked
                }
            }

            for (int i = 0; i < count; i++)
            {
                try
                {
                    var result = consumer.Consume(TimeSpan.FromSeconds(2));
                    if (result == null || result.IsPartitionEOF) continue;

                    messages.Add(ToMessageRecord(result));
                }
                catch (ConsumeException) { break; }
            }

            consumer.Close();
            return Task.FromResult(messages);
        }

        public async Task<bool> SendMessageAsync(string bootstrapServers, string topic, string key, string value,
            int? partition = null, Dictionary<string, string> headers = null, Action<ProducerConfig> configAction = null)
        {
            var config = BuildProducerConfig(bootstrapServers, configAction);
            using var producer = new ProducerBuilder<string, string>(config).Build();

            var msg = new Message<string, string>
            {
                Key = key ?? string.Empty,
                Value = value ?? string.Empty
            };

            if (headers != null)
            {
                msg.Headers = new Headers();
                foreach (var h in headers)
                {
                    msg.Headers.Add(h.Key, System.Text.Encoding.UTF8.GetBytes(h.Value ?? string.Empty));
                }
            }

            DeliveryResult<string, string> result;
            if (partition.HasValue)
                result = await producer.ProduceAsync(new TopicPartition(topic, new Partition(partition.Value)), msg);
            else
                result = await producer.ProduceAsync(topic, msg);

            if (result.Status == PersistenceStatus.NotPersisted)
                throw new Exception($"Message not persisted. Status: {result.Status}");

            return true;
        }

        public async Task<bool> TestConnectionAsync(string bootstrapServers, Action<AdminClientConfig> configAction = null)
        {
            try
            {
                var config = BuildAdminConfig(bootstrapServers, configAction);
                using var adminClient = new AdminClientBuilder(config).Build();
                var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5));
                return await Task.FromResult(metadata.Brokers.Count > 0);
            }
            catch
            {
                return false;
            }
        }

        public Task<bool> ResetConsumerGroupOffsetAsync(string bootstrapServers, string groupId, string topic,
            long? offset, Action<AdminClientConfig> configAction = null)
        {
            var config = BuildAdminConfig(bootstrapServers, configAction);
            using var adminClient = new AdminClientBuilder(config).Build();

            var metadata = adminClient.GetMetadata(topic, TimeSpan.FromSeconds(10));
            var topicMeta = metadata.Topics.FirstOrDefault(t => t.Topic == topic);
            if (topicMeta == null) throw new Exception($"Topic '{topic}' not found.");

            using var consumer = new ConsumerBuilder<Ignore, Ignore>(new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = groupId,
                EnableAutoCommit = false
            }).Build();

            var tps = topicMeta.Partitions.Select(p =>
                new TopicPartition(topic, p.PartitionId)).ToList();

            var targetOffset = offset ?? 0L;
            foreach (var tp in tps)
            {
                consumer.Assign(new TopicPartitionOffset(tp, targetOffset));
                consumer.Commit();
            }

            consumer.Close();
            return Task.FromResult(true);
        }

        private static MessageRecord ToMessageRecord(ConsumeResult<string, string> result)
        {
            return new MessageRecord
            {
                Topic = result.Topic,
                Partition = result.Partition,
                Offset = result.Offset.Value,
                Key = result.Message.Key ?? "",
                Value = result.Message.Value ?? "",
                Timestamp = result.Message.Timestamp.UnixTimestampMs,
                TimestampType = result.Message.Timestamp.Type.ToString(),
                Headers = result.Message.Headers?
                    .Select(h => new { h.Key, Value = h.GetValueBytes() })
                    .Where(h => h.Value != null)
                    .ToDictionary(h => h.Key, h => System.Text.Encoding.UTF8.GetString(h.Value))
                    ?? new Dictionary<string, string>()
            };
        }
    }
}