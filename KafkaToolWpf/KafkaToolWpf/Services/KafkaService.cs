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
        private static void CopyClientConfigValues(ClientConfig source, ClientConfig target)
        {
            foreach (var entry in source)
            {
                target.Set(entry.Key, entry.Value);
            }
        }

        private ConsumerConfig BuildConsumerConfigFromAdmin(AdminClientConfig adminConfig, string groupId,
            Action<ConsumerConfig> configAction = null)
        {
            var config = BuildConsumerConfig(adminConfig.BootstrapServers, groupId);
            CopyClientConfigValues(adminConfig, config);
            configAction?.Invoke(config);
            return config;
        }

        private AdminClientConfig BuildAdminConfigFromConsumer(ConsumerConfig consumerConfig)
        {
            var config = BuildAdminConfig(consumerConfig.BootstrapServers);
            CopyClientConfigValues(consumerConfig, config);
            return config;
        }

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
            var clusterDescription = adminClient.DescribeClusterAsync(new DescribeClusterOptions()).GetAwaiter().GetResult();

            var cluster = new ClusterInfo
            {
                ClusterId = clusterDescription.ClusterId ?? metadata.OriginatingBrokerName,
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
            var adminConfig = BuildAdminConfig(bootstrapServers, configAction);
            using var adminClient = new AdminClientBuilder(adminConfig).Build();

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

            // Reuse the same security settings so topic detail works for SASL/SSL clusters too.
            var consumerConfig = BuildConsumerConfigFromAdmin(adminConfig, $"kafka-topic-detail-{Guid.NewGuid():N}", cfg =>
            {
                cfg.EnableAutoCommit = false;
                cfg.AutoOffsetReset = AutoOffsetReset.Earliest;
            });

            using var consumer = new ConsumerBuilder<Ignore, Ignore>(consumerConfig).Build();

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

        public async Task<bool> UpdateTopicConfigsAsync(string bootstrapServers, string topicName,
            Dictionary<string, string> configs, Action<AdminClientConfig> configAction = null)
        {
            var config = BuildAdminConfig(bootstrapServers, configAction);
            using var adminClient = new AdminClientBuilder(config).Build();

            var topicConfigs = configs?
                .Where(kv => !string.IsNullOrWhiteSpace(kv.Key))
                .Select(kv => new ConfigEntry
                {
                    Name = kv.Key.Trim(),
                    Value = kv.Value?.Trim() ?? string.Empty,
                    IncrementalOperation = AlterConfigOpType.Set
                })
                .ToList() ?? new List<ConfigEntry>();

            var resource = new ConfigResource
            {
                Type = ResourceType.Topic,
                Name = topicName
            };

            await adminClient.IncrementalAlterConfigsAsync(
                new Dictionary<ConfigResource, List<ConfigEntry>>
                {
                    [resource] = topicConfigs
                },
                new IncrementalAlterConfigsOptions
                {
                    RequestTimeout = TimeSpan.FromSeconds(10)
                });

            return true;
        }

        public async Task<bool> IncreaseTopicPartitionsAsync(string bootstrapServers, string topicName, int partitionCount,
            Action<AdminClientConfig> configAction = null)
        {
            var config = BuildAdminConfig(bootstrapServers, configAction);
            using var adminClient = new AdminClientBuilder(config).Build();

            await adminClient.CreatePartitionsAsync(
                new[]
                {
                    new PartitionsSpecification
                    {
                        Topic = topicName,
                        IncreaseTo = partitionCount
                    }
                },
                new CreatePartitionsOptions
                {
                    RequestTimeout = TimeSpan.FromSeconds(10),
                    OperationTimeout = TimeSpan.FromSeconds(10)
                });

            return true;
        }

        public async Task<bool> DeleteTopicAsync(string bootstrapServers, string topicName, Action<AdminClientConfig> configAction = null)
        {
            var config = BuildAdminConfig(bootstrapServers, configAction);
            using var adminClient = new AdminClientBuilder(config).Build();

            await adminClient.DeleteTopicsAsync(new[] { topicName }, null);
            return true;
        }

        public async Task<List<ConsumerGroupInfo>> GetConsumerGroupsAsync(string bootstrapServers, Action<AdminClientConfig> configAction = null)
        {
            var config = BuildAdminConfig(bootstrapServers, configAction);
            using var adminClient = new AdminClientBuilder(config).Build();

            var groups = adminClient.ListGroups(TimeSpan.FromSeconds(10))
                .Where(g => g.ProtocolType == "consumer")
                .ToList();

            if (groups.Count == 0)
            {
                return new List<ConsumerGroupInfo>();
            }

            var descriptions = await adminClient.DescribeConsumerGroupsAsync(
                groups.Select(g => g.Group),
                new DescribeConsumerGroupsOptions
                {
                    RequestTimeout = TimeSpan.FromSeconds(10)
                });

            var descriptionMap = descriptions.ConsumerGroupDescriptions
                .ToDictionary(d => d.GroupId, d => d);

            var result = groups.Select(g =>
            {
                descriptionMap.TryGetValue(g.Group, out var description);
                var members = description?.Members?.Select(m => new MemberInfo
                {
                    MemberId = m.ConsumerId,
                    ClientId = m.ClientId,
                    Host = m.Host,
                    AssignedPartitions = FormatAssignedPartitions(m.Assignment?.TopicPartitions)
                }).ToList() ?? new List<MemberInfo>();

                return new ConsumerGroupInfo
                {
                    GroupId = g.Group,
                    State = description?.State.ToString() ?? g.State,
                    ProtocolType = g.ProtocolType,
                    Members = members
                };
            }).ToList();

            return result;
        }

        public async Task<List<ConsumerGroupDetail>> GetConsumerGroupDetailsAsync(string bootstrapServers, string groupId, Action<AdminClientConfig> configAction = null)
        {
            var adminConfig = BuildAdminConfig(bootstrapServers, configAction);
            using var adminClient = new AdminClientBuilder(adminConfig).Build();

            var descriptionResult = await adminClient.DescribeConsumerGroupsAsync(
                new[] { groupId },
                new DescribeConsumerGroupsOptions
                {
                    RequestTimeout = TimeSpan.FromSeconds(10)
                });

            var description = descriptionResult.ConsumerGroupDescriptions.FirstOrDefault();
            if (description == null)
            {
                return new List<ConsumerGroupDetail>();
            }

            if (description.Error.Code != ErrorCode.NoError)
            {
                throw new Exception(description.Error.Reason);
            }

            var memberAssignments = description.Members
                .SelectMany(member => (member.Assignment?.TopicPartitions ?? Enumerable.Empty<TopicPartition>())
                    .Select(tp => new
                    {
                        TopicPartition = tp,
                        member.ClientId,
                        member.Host
                    }))
                .GroupBy(x => x.TopicPartition)
                .ToDictionary(g => g.Key, g => g.First());

            var topicPartitions = memberAssignments.Keys.ToList();
            if (topicPartitions.Count == 0)
            {
                var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
                topicPartitions = metadata.Topics
                    .Where(t => t.Error.Code == ErrorCode.NoError && !t.Topic.StartsWith("__"))
                    .SelectMany(t => t.Partitions.Select(p => new TopicPartition(t.Topic, p.PartitionId)))
                    .ToList();
            }

            if (topicPartitions.Count == 0)
            {
                return new List<ConsumerGroupDetail>();
            }

            var offsetsResult = await adminClient.ListConsumerGroupOffsetsAsync(
                new[] { new ConsumerGroupTopicPartitions(groupId, topicPartitions) },
                new ListConsumerGroupOffsetsOptions
                {
                    RequestTimeout = TimeSpan.FromSeconds(10)
                });

            var consumerConfig = BuildConsumerConfigFromAdmin(adminConfig, $"kafka-group-detail-{Guid.NewGuid():N}", cfg =>
            {
                cfg.EnableAutoCommit = false;
                cfg.AutoOffsetReset = AutoOffsetReset.Earliest;
            });

            using var consumer = new ConsumerBuilder<Ignore, Ignore>(consumerConfig).Build();
            var details = new List<ConsumerGroupDetail>();

            foreach (var partitionOffset in offsetsResult.SelectMany(r => r.Partitions))
            {
                if (partitionOffset.Error.Code != ErrorCode.NoError || partitionOffset.Offset == Offset.Unset)
                {
                    continue;
                }

                try
                {
                    var watermarks = consumer.GetWatermarkOffsets(partitionOffset.TopicPartition);
                    memberAssignments.TryGetValue(partitionOffset.TopicPartition, out var owner);

                    details.Add(new ConsumerGroupDetail
                    {
                        GroupId = groupId,
                        Topic = partitionOffset.TopicPartition.Topic,
                        Partition = partitionOffset.TopicPartition.Partition,
                        CurrentOffset = partitionOffset.Offset.Value,
                        LogEndOffset = watermarks.High,
                        ClientId = owner?.ClientId ?? string.Empty,
                        Host = owner?.Host ?? string.Empty
                    });
                }
                catch
                {
                    // Skip partitions whose watermark cannot be read.
                }
            }

            consumer.Close();
            return details
                .OrderBy(d => d.Topic)
                .ThenBy(d => d.Partition)
                .ToList();
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

            var adminConfig = BuildAdminConfigFromConsumer(config);
            using var adminClient = new AdminClientBuilder(adminConfig).Build();
            var metadata = adminClient.GetMetadata(topic, TimeSpan.FromSeconds(10));
            var topicMeta = metadata.Topics.FirstOrDefault(t => t.Topic == topic);
            if (topicMeta == null) return Task.FromResult(new List<MessageRecord>());

            var messages = new List<MessageRecord>();
            using var consumer = new ConsumerBuilder<string, string>(config).Build();
            var assignments = new List<TopicPartitionOffset>();

            foreach (var p in topicMeta.Partitions)
            {
                var tp = new TopicPartition(topic, p.PartitionId);
                var tsOffset = new Timestamp(timestamp);

                try
                {
                    var offsets = consumer.OffsetsForTimes(
                        new[] { new TopicPartitionTimestamp(tp, tsOffset) },
                        TimeSpan.FromSeconds(10));

                    if (offsets.Count > 0 && offsets[0].Offset != Offset.Unset)
                    {
                        assignments.Add(new TopicPartitionOffset(offsets[0].TopicPartition, offsets[0].Offset));
                    }
                }
                catch
                {
                    // Skip partitions that can't be offset-seeked
                }
            }

            if (assignments.Count == 0)
            {
                consumer.Close();
                return Task.FromResult(messages);
            }

            consumer.Assign(assignments);

            int emptyPolls = 0;
            int maxEmptyPolls = Math.Max(assignments.Count * 3, 6);

            while (messages.Count < count && emptyPolls < maxEmptyPolls)
            {
                try
                {
                    var result = consumer.Consume(TimeSpan.FromSeconds(2));
                    if (result == null || result.IsPartitionEOF)
                    {
                        emptyPolls++;
                        continue;
                    }

                    messages.Add(ToMessageRecord(result));
                    emptyPolls = 0;
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
            var adminConfig = BuildAdminConfig(bootstrapServers, configAction);
            using var adminClient = new AdminClientBuilder(adminConfig).Build();

            var metadata = adminClient.GetMetadata(topic, TimeSpan.FromSeconds(10));
            var topicMeta = metadata.Topics.FirstOrDefault(t => t.Topic == topic);
            if (topicMeta == null) throw new Exception($"Topic '{topic}' not found.");

            var consumerConfig = BuildConsumerConfigFromAdmin(adminConfig, groupId, cfg =>
            {
                cfg.EnableAutoCommit = false;
                cfg.AutoOffsetReset = AutoOffsetReset.Earliest;
            });

            using var consumer = new ConsumerBuilder<Ignore, Ignore>(consumerConfig).Build();

            var topicPartitions = topicMeta.Partitions
                .Select(p => new TopicPartition(topic, p.PartitionId))
                .ToList();

            var targetOffset = offset ?? 0L;
            var offsetsToCommit = new List<TopicPartitionOffset>(topicPartitions.Count);

            foreach (var tp in topicPartitions)
            {
                offsetsToCommit.Add(new TopicPartitionOffset(tp, new Offset(targetOffset)));
            }

            consumer.Commit(offsetsToCommit);
            consumer.Close();
            return Task.FromResult(true);
        }

        public Task<bool> ResetConsumerGroupOffsetsAsync(string bootstrapServers, string groupId, List<ConsumerGroupDetail> details,
            string strategy, long? offset, DateTime? timestamp, Action<AdminClientConfig> configAction = null)
        {
            if (details == null || details.Count == 0)
            {
                return Task.FromResult(false);
            }

            var adminConfig = BuildAdminConfig(bootstrapServers, configAction);
            var consumerConfig = BuildConsumerConfigFromAdmin(adminConfig, groupId, cfg =>
            {
                cfg.EnableAutoCommit = false;
                cfg.AutoOffsetReset = AutoOffsetReset.Earliest;
            });

            using var consumer = new ConsumerBuilder<Ignore, Ignore>(consumerConfig).Build();
            var offsetsToCommit = new List<TopicPartitionOffset>(details.Count);

            foreach (var detail in details)
            {
                var tp = new TopicPartition(detail.Topic, detail.Partition);
                long targetOffset;
                switch (strategy)
                {
                    case "earliest":
                        targetOffset = consumer.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(10)).Low;
                        break;
                    case "latest":
                        targetOffset = consumer.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(10)).High;
                        break;
                    case "specific":
                        targetOffset = offset ?? 0L;
                        break;
                    case "timestamp":
                        targetOffset = ResolveOffsetByTimestamp(consumer, tp, timestamp);
                        break;
                    default:
                        throw new Exception($"Unsupported reset strategy: {strategy}");
                }

                offsetsToCommit.Add(new TopicPartitionOffset(tp, new Offset(targetOffset)));
            }

            consumer.Commit(offsetsToCommit);
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

        private static List<string> FormatAssignedPartitions(IEnumerable<TopicPartition> topicPartitions)
        {
            return topicPartitions?
                .Select(tp => $"{tp.Topic}[{tp.Partition}]")
                .ToList() ?? new List<string>();
        }

        private static long ResolveOffsetByTimestamp(IConsumer<Ignore, Ignore> consumer, TopicPartition topicPartition, DateTime? timestamp)
        {
            if (timestamp == null)
            {
                throw new Exception("Timestamp reset strategy requires a timestamp value.");
            }

            var result = consumer.OffsetsForTimes(
                new[] { new TopicPartitionTimestamp(topicPartition, new Timestamp(timestamp.Value)) },
                TimeSpan.FromSeconds(10));

            if (result.Count == 0 || result[0].Offset == Offset.Unset)
            {
                return consumer.QueryWatermarkOffsets(topicPartition, TimeSpan.FromSeconds(10)).High;
            }

            return result[0].Offset.Value;
        }
    }
}
