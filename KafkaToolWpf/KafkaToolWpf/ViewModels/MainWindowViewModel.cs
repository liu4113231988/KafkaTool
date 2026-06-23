using Confluent.Kafka;
using KafkaToolWpf.Helpers;
using KafkaToolWpf.Models;
using KafkaToolWpf.Services;
using Prism.Commands;
using Prism.Dialogs;
using Prism.Mvvm;
using Microsoft.Win32;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Threading;
using SysTimer = System.Timers.Timer;

namespace KafkaToolWpf.ViewModels
{
    public partial class MainWindowViewModel : BindableBase
    {
        private const int MaxConsumerMessageChars = 50000;
        private static readonly string ConnectionStorePath = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
            "KafkaTool",
            "connections.json");

        private readonly IKafkaService _kafkaService;
        private readonly IDialogService _dialogService;
        private readonly IAppLogger _logger;

        private CancellationTokenSource _consumerCts;
        private CancellationTokenSource _clearCts;
        private SysTimer _tooltipTimer;
        private List<MessageRecord> _lastBrowsedMessages = new();
        private string _lastBrowseTopic;
        private int? _lastBrowsePartition;

        #region Connection

        private string _kafkaBootstrapServer = "localhost:9092";
        public string KafkaBootstrapServer
        {
            get => _kafkaBootstrapServer;
            set => SetProperty(ref _kafkaBootstrapServer, value);
        }

        private ObservableCollection<ConnectionConfig> _connections = new();
        public ObservableCollection<ConnectionConfig> Connections
        {
            get => _connections;
            set => SetProperty(ref _connections, value);
        }

        private ConnectionConfig _currentConnection;
        public ConnectionConfig CurrentConnection
        {
            get => _currentConnection;
            set
            {
                SetProperty(ref _currentConnection, value);
                if (value != null)
                    KafkaBootstrapServer = value.BootstrapServers;
            }
        }

        private string _connectionStatus;
        public string ConnectionStatus
        {
            get => _connectionStatus;
            set => SetProperty(ref _connectionStatus, value);
        }

        #endregion

        #region Producer

        private string _producerTopic;
        public string ProducerTopic
        {
            get => _producerTopic;
            set => SetProperty(ref _producerTopic, value);
        }

        private string _producerKey;
        public string ProducerKey
        {
            get => _producerKey;
            set => SetProperty(ref _producerKey, value);
        }

        private string _producerMessage;
        public string ProducerMessage
        {
            get => _producerMessage;
            set => SetProperty(ref _producerMessage, value);
        }

        private int? _producerPartition;
        public int? ProducerPartition
        {
            get => _producerPartition;
            set => SetProperty(ref _producerPartition, value);
        }

        private string _producerHeaders;
        public string ProducerHeaders
        {
            get => _producerHeaders;
            set => SetProperty(ref _producerHeaders, value);
        }

        private string _producerStatus;
        public string ProducerStatus
        {
            get => _producerStatus;
            set => SetProperty(ref _producerStatus, value);
        }

        private bool _producerMessageIsJson;
        public bool ProducerMessageIsJson
        {
            get => _producerMessageIsJson;
            set
            {
                if (SetProperty(ref _producerMessageIsJson, value))
                {
                    RaisePropertyChanged(nameof(ProducerPayloadTypeText));
                }
            }
        }

        public string ProducerPayloadTypeText => ProducerMessageIsJson ? "JSON" : "普通文本";

        #endregion

        #region Consumer

        private string _consumerTopic;
        public string ConsumerTopic
        {
            get => _consumerTopic;
            set => SetProperty(ref _consumerTopic, value);
        }

        private string _consumerGroup;
        public string ConsumerGroup
        {
            get => _consumerGroup;
            set => SetProperty(ref _consumerGroup, value);
        }

        private string _consumerMessage;
        public string ConsumerMessage
        {
            get => _consumerMessage;
            set => SetProperty(ref _consumerMessage, value);
        }

        private string _consumerTooltipMsg;
        public string ConsumerTooltipMsg
        {
            get => _consumerTooltipMsg;
            set => SetProperty(ref _consumerTooltipMsg, value);
        }

        private string _consumerErrorMessage;
        public string ConsumerErrorMessage
        {
            get => _consumerErrorMessage;
            set => SetProperty(ref _consumerErrorMessage, value);
        }

        private bool _isConsumerReadonly;
        public bool IsConsumerReadonly
        {
            get => _isConsumerReadonly;
            set => SetProperty(ref _isConsumerReadonly, value);
        }

        private bool _isConsuming;
        public bool IsConsuming
        {
            get => _isConsuming;
            set => SetProperty(ref _isConsuming, value);
        }

        #endregion

        #region Topics

        private ObservableCollection<TopicInfo> _topicList = new();
        public ObservableCollection<TopicInfo> TopicList
        {
            get => _topicList;
            set => SetProperty(ref _topicList, value);
        }

        private TopicInfo _selectedTopic;
        public TopicInfo SelectedTopic
        {
            get => _selectedTopic;
            set => SetProperty(ref _selectedTopic, value);
        }

        private string _topicFilter;
        public string TopicFilter
        {
            get => _topicFilter;
            set
            {
                SetProperty(ref _topicFilter, value);
                FilterTopicList();
            }
        }

        public ObservableCollection<TopicInfo> FilteredTopicList { get; set; } = new();

        private bool _isTopicLoading;
        public bool IsTopicLoading
        {
            get => _isTopicLoading;
            set => SetProperty(ref _isTopicLoading, value);
        }

        private string _topicStatus;
        public string TopicStatus
        {
            get => _topicStatus;
            set => SetProperty(ref _topicStatus, value);
        }

        // Topic detail
        private TopicInfo _topicDetail;
        public TopicInfo TopicDetail
        {
            get => _topicDetail;
            set => SetProperty(ref _topicDetail, value);
        }

        private PartitionInfo _selectedPartition;
        public PartitionInfo SelectedPartition
        {
            get => _selectedPartition;
            set => SetProperty(ref _selectedPartition, value);
        }

        private string _topicConfigEntries;
        public string TopicConfigEntries
        {
            get => _topicConfigEntries;
            set => SetProperty(ref _topicConfigEntries, value);
        }

        private int _topicTargetPartitionCount;
        public int TopicTargetPartitionCount
        {
            get => _topicTargetPartitionCount;
            set => SetProperty(ref _topicTargetPartitionCount, value);
        }

        private int _selectedTabIndex;
        public int SelectedTabIndex
        {
            get => _selectedTabIndex;
            set => SetProperty(ref _selectedTabIndex, value);
        }

        #endregion

        #region Consumer Groups

        private ObservableCollection<ConsumerGroupInfo> _consumerGroups = new();
        public ObservableCollection<ConsumerGroupInfo> ConsumerGroups
        {
            get => _consumerGroups;
            set => SetProperty(ref _consumerGroups, value);
        }

        private ConsumerGroupInfo _selectedConsumerGroup;
        public ConsumerGroupInfo SelectedConsumerGroup
        {
            get => _selectedConsumerGroup;
            set => SetProperty(ref _selectedConsumerGroup, value);
        }

        private ObservableCollection<ConsumerGroupDetail> _consumerGroupDetails = new();
        public ObservableCollection<ConsumerGroupDetail> ConsumerGroupDetails
        {
            get => _consumerGroupDetails;
            set => SetProperty(ref _consumerGroupDetails, value);
        }

        private string _consumerGroupStatus;
        public string ConsumerGroupStatus
        {
            get => _consumerGroupStatus;
            set => SetProperty(ref _consumerGroupStatus, value);
        }

        private long _totalLag;
        public long TotalLag
        {
            get => _totalLag;
            set => SetProperty(ref _totalLag, value);
        }

        private string _resetOffsetStrategy = "earliest";
        public string ResetOffsetStrategy
        {
            get => _resetOffsetStrategy;
            set => SetProperty(ref _resetOffsetStrategy, value);
        }

        private long? _resetOffsetValue;
        public long? ResetOffsetValue
        {
            get => _resetOffsetValue;
            set => SetProperty(ref _resetOffsetValue, value);
        }

        private DateTime _resetOffsetTimestamp = DateTime.Now.AddHours(-1);
        public DateTime ResetOffsetTimestamp
        {
            get => _resetOffsetTimestamp;
            set => SetProperty(ref _resetOffsetTimestamp, value);
        }

        private string _resetOffsetTimeText = DateTime.Now.AddHours(-1).ToString("HH:mm:ss");
        public string ResetOffsetTimeText
        {
            get => _resetOffsetTimeText;
            set => SetProperty(ref _resetOffsetTimeText, value);
        }

        #endregion

        #region Cluster

        private ClusterInfo _clusterInfo;
        public ClusterInfo ClusterInfo
        {
            get => _clusterInfo;
            set => SetProperty(ref _clusterInfo, value);
        }

        private string _clusterStatus;
        public string ClusterStatus
        {
            get => _clusterStatus;
            set => SetProperty(ref _clusterStatus, value);
        }

        #endregion

        #region Message Browser

        private string _browseTopic;
        public string BrowseTopic
        {
            get => _browseTopic;
            set => SetProperty(ref _browseTopic, value);
        }

        private int? _browsePartition;
        public int? BrowsePartition
        {
            get => _browsePartition;
            set => SetProperty(ref _browsePartition, value);
        }

        private long? _browseOffset;
        public long? BrowseOffset
        {
            get => _browseOffset;
            set => SetProperty(ref _browseOffset, value);
        }

        private int _browseCount = 10;
        public int BrowseCount
        {
            get => _browseCount;
            set => SetProperty(ref _browseCount, value);
        }

        private DateTime _browseTimestamp = DateTime.Now.AddHours(-1);
        public DateTime BrowseTimestamp
        {
            get => _browseTimestamp;
            set => SetProperty(ref _browseTimestamp, value);
        }

        private bool _browseByTimestamp;
        public bool BrowseByTimestamp
        {
            get => _browseByTimestamp;
            set => SetProperty(ref _browseByTimestamp, value);
        }

        private string _browseGroupId;
        public string BrowseGroupId
        {
            get => _browseGroupId;
            set => SetProperty(ref _browseGroupId, value);
        }

        private string _browseTimeText = DateTime.Now.AddHours(-1).ToString("HH:mm:ss");
        public string BrowseTimeText
        {
            get => _browseTimeText;
            set => SetProperty(ref _browseTimeText, value);
        }

        public IReadOnlyList<string> ResetOffsetStrategies { get; } = new[]
        {
            "earliest",
            "latest",
            "specific",
            "timestamp"
        };

        public IReadOnlyList<KeyValuePair<string, string>> ResetOffsetStrategyOptions { get; } = new[]
        {
            new KeyValuePair<string, string>("earliest", "重置到最早可用偏移"),
            new KeyValuePair<string, string>("latest", "重置到最新偏移"),
            new KeyValuePair<string, string>("specific", "重置到指定偏移"),
            new KeyValuePair<string, string>("timestamp", "重置到指定时间")
        };

        #endregion

        #region Commands

        public ICommand SendMessageCommand { get; }
        public ICommand FormatProducerJsonCommand { get; }
        public ICommand ValidateProducerPayloadCommand { get; }
        public ICommand SubscribeMessageCommand { get; }
        public ICommand CancelSubscribeCommand { get; }
        public ICommand WindowClosingCommand { get; }
        public ICommand AcquireTopicListCommand { get; }
        public ICommand RefreshTopicListCommand { get; }
        public ICommand CreateTopicCommand { get; }
        public ICommand DeleteTopicCommand { get; }
        public ICommand ViewTopicDetailCommand { get; }
        public ICommand SaveTopicConfigsCommand { get; }
        public ICommand ExpandTopicPartitionsCommand { get; }
        public ICommand RefreshConsumerGroupsCommand { get; }
        public ICommand ViewConsumerGroupDetailCommand { get; }
        public ICommand DeleteConsumerGroupCommand { get; }
        public ICommand ResetConsumerGroupOffsetCommand { get; }
        public ICommand RefreshClusterInfoCommand { get; }
        public ICommand BrowseMessagesCommand { get; }
        public ICommand ContinueBrowseMessagesCommand { get; }
        public ICommand TestConnectionCommand { get; }
        public ICommand ManageConnectionsCommand { get; }
        public ICommand SelectConnectionCommand { get; }

        #endregion

        public MainWindowViewModel(IKafkaService kafkaService, IDialogService dialogService, IAppLogger logger)
        {
            _kafkaService = kafkaService;
            _dialogService = dialogService;
            _logger = logger;

            _consumerCts = new CancellationTokenSource();
            _clearCts = new CancellationTokenSource();
            IsConsumerReadonly = false;
            IsConsuming = false;

            LoadConnectionsFromDisk();
            EnsureDefaultConnection();
            Connections.CollectionChanged += Connections_CollectionChanged;
            CurrentConnection = Connections[0];

            // Init commands
            SendMessageCommand = new AsyncDelegateCommand(SendMessageAsync);
            FormatProducerJsonCommand = new DelegateCommand(FormatProducerJson);
            ValidateProducerPayloadCommand = new DelegateCommand(ValidateProducerPayloadFromUi);
            SubscribeMessageCommand = new AsyncDelegateCommand<TextBox>(SubscribeMessageAsync);
            CancelSubscribeCommand = new DelegateCommand(CancelSubscribe);
            WindowClosingCommand = new DelegateCommand(WindowClosing);
            AcquireTopicListCommand = new AsyncDelegateCommand(AcquireTopicListAsync);
            RefreshTopicListCommand = new AsyncDelegateCommand(RefreshTopicListAsync);
            CreateTopicCommand = new AsyncDelegateCommand(CreateTopicAsync);
            DeleteTopicCommand = new AsyncDelegateCommand(DeleteTopicAsync);
            ViewTopicDetailCommand = new AsyncDelegateCommand(ViewTopicDetailAsync);
            SaveTopicConfigsCommand = new AsyncDelegateCommand(SaveTopicConfigsAsync);
            ExpandTopicPartitionsCommand = new AsyncDelegateCommand(ExpandTopicPartitionsAsync);
            RefreshConsumerGroupsCommand = new AsyncDelegateCommand(RefreshConsumerGroupsAsync);
            ViewConsumerGroupDetailCommand = new AsyncDelegateCommand(ViewConsumerGroupDetailAsync);
            DeleteConsumerGroupCommand = new AsyncDelegateCommand(DeleteConsumerGroupAsync);
            ResetConsumerGroupOffsetCommand = new AsyncDelegateCommand(ResetConsumerGroupOffsetAsync);
            RefreshClusterInfoCommand = new AsyncDelegateCommand(RefreshClusterInfoAsync);
            BrowseMessagesCommand = new AsyncDelegateCommand(BrowseMessagesAsync);
            ContinueBrowseMessagesCommand = new AsyncDelegateCommand(ContinueBrowseMessagesAsync);
            TestConnectionCommand = new AsyncDelegateCommand(TestConnectionAsync);
            ManageConnectionsCommand = new AsyncDelegateCommand(ManageConnectionsAsync);
            SelectConnectionCommand = new DelegateCommand<ConnectionConfig>(SelectConnection);

            // Tooltip timer
            _tooltipTimer = new SysTimer(5000);
            _tooltipTimer.Elapsed += (s, e) => ConsumerTooltipMsg = "";

            _logger.Info($"主界面已初始化，当前连接数: {Connections.Count}。");
        }

        #region Connection Management

        private void SelectConnection(ConnectionConfig connection)
        {
            if (connection != null)
            {
                CurrentConnection = connection;
                ConnectionStatus = $"已连接: {connection.Name} ({connection.BootstrapServers})";
                _logger.Info($"切换当前连接: {connection.Name} ({connection.BootstrapServers})。");
            }
        }

        private void LoadConnectionsFromDisk()
        {
            try
            {
                if (!File.Exists(ConnectionStorePath))
                {
                    return;
                }

                var json = File.ReadAllText(ConnectionStorePath);
                var loaded = JsonSerializer.Deserialize<List<ConnectionConfig>>(json);
                if (loaded == null || loaded.Count == 0)
                {
                    return;
                }

                Connections.Clear();
                foreach (var connection in loaded)
                {
                    Connections.Add(connection);
                }

                _logger.Info($"已加载 {Connections.Count} 个连接配置。");
            }
            catch (Exception ex)
            {
                ConnectionStatus = "连接配置加载失败，已回退到默认配置。";
                _logger.Error("加载本地连接配置失败。", ex);
            }
        }

        private void Connections_CollectionChanged(object sender, NotifyCollectionChangedEventArgs e)
        {
            RaisePropertyChanged(nameof(Connections));
        }

        private void SaveConnectionsToDisk()
        {
            var directory = Path.GetDirectoryName(ConnectionStorePath);
            if (!string.IsNullOrWhiteSpace(directory))
            {
                Directory.CreateDirectory(directory);
            }

            var json = JsonSerializer.Serialize(Connections, new JsonSerializerOptions
            {
                WriteIndented = true
            });
            File.WriteAllText(ConnectionStorePath, json);
            _logger.Info($"连接配置已保存到 {ConnectionStorePath}。");
        }

        private void EnsureDefaultConnection()
        {
            if (Connections.Count > 0)
            {
                return;
            }

            Connections.Add(new ConnectionConfig
            {
                Name = "本地 Kafka",
                BootstrapServers = "localhost:9092"
            });
            _logger.Info("未找到本地连接配置，已创建默认连接。");
        }

        private async Task ManageConnectionsAsync()
        {
            var currentConnectionName = CurrentConnection?.Name;
            var param = new DialogParameters
            {
                { "connections", Connections },
                { "selectedConnection", currentConnectionName }
            };

            var result = await _dialogService.ShowDialogAsync("ConnectionConfigDialogWindow", param);

            if (result.Result == ButtonResult.OK || result.Result == ButtonResult.None)
            {
                var action = result.Parameters.GetValue<string>("action");
                if (action == "save" || action == "select")
                {
                    var savedConnections = result.Parameters.GetValue<ObservableCollection<ConnectionConfig>>("connections");
                    if (savedConnections != null)
                    {
                        Connections.Clear();
                        foreach (var c in savedConnections)
                            Connections.Add(c);
                        EnsureDefaultConnection();
                        SaveConnectionsToDisk();
                        _logger.Info($"连接管理已保存，当前连接数: {Connections.Count}。");

                        var restoredCurrent = Connections.FirstOrDefault(c => c.Name == currentConnectionName)
                            ?? Connections.FirstOrDefault();

                        if (restoredCurrent != null)
                        {
                            CurrentConnection = restoredCurrent;
                            KafkaBootstrapServer = restoredCurrent.BootstrapServers;
                        }
                        else
                        {
                            CurrentConnection = null;
                        }
                    }

                    if (action == "select")
                    {
                        var selectedName = result.Parameters.GetValue<string>("selectedConnection");
                        var selected = Connections.FirstOrDefault(c => c.Name == selectedName);
                        if (selected != null)
                            SelectConnection(selected);
                    }
                }
            }
        }

        private async Task TestConnectionAsync()
        {
            ConnectionStatus = "正在测试连接...";
            try
            {
                var isConnected = await _kafkaService.TestConnectionAsync(KafkaBootstrapServer, cfg =>
                {
                    ApplySaslConfig(cfg);
                });

                ConnectionStatus = isConnected
                    ? $"✓ 连接成功 - {KafkaBootstrapServer}"
                    : $"✗ 连接失败 - {KafkaBootstrapServer}";
                _logger.Info($"连接测试完成: {KafkaBootstrapServer}, 结果: {isConnected}。");
            }
            catch (Exception ex)
            {
                ConnectionStatus = $"✗ 连接失败: {ex.Message}";
                _logger.Error($"连接测试失败: {KafkaBootstrapServer}。", ex);
            }
        }

        private void ApplySslFileConfig(ClientConfig config)
        {
            if (CurrentConnection == null)
            {
                return;
            }

            if (!string.IsNullOrWhiteSpace(CurrentConnection.SslCaLocation))
            {
                config.SslCaLocation = CurrentConnection.SslCaLocation;
            }

            if (!string.IsNullOrWhiteSpace(CurrentConnection.SslCertificateLocation))
            {
                config.SslCertificateLocation = CurrentConnection.SslCertificateLocation;
            }

            if (!string.IsNullOrWhiteSpace(CurrentConnection.SslKeyLocation))
            {
                config.SslKeyLocation = CurrentConnection.SslKeyLocation;
            }
        }

        private void ApplySaslConfig(AdminClientConfig config)
        {
            if (CurrentConnection?.UseSasl == true)
            {
                config.SecurityProtocol = CurrentConnection.UseSsl
                    ? SecurityProtocol.SaslSsl
                    : SecurityProtocol.SaslPlaintext;
                config.SaslMechanism = CurrentConnection.SaslMechanism switch
                {
                    "PLAIN" => Confluent.Kafka.SaslMechanism.Plain,
                    "SCRAM-SHA-256" => Confluent.Kafka.SaslMechanism.ScramSha256,
                    "SCRAM-SHA-512" => Confluent.Kafka.SaslMechanism.ScramSha512,
                    "GSSAPI" => Confluent.Kafka.SaslMechanism.Gssapi,
                    _ => Confluent.Kafka.SaslMechanism.Plain
                };
                config.SaslUsername = CurrentConnection.SaslUsername;
                config.SaslPassword = CurrentConnection.SaslPassword;
            }
            else if (CurrentConnection?.UseSsl == true)
            {
                config.SecurityProtocol = SecurityProtocol.Ssl;
            }

            ApplySslFileConfig(config);

            if (CurrentConnection?.SslSkipVerify == true)
            {
                config.SslEndpointIdentificationAlgorithm = Confluent.Kafka.SslEndpointIdentificationAlgorithm.None;
            }
        }

        private void ApplySaslConfig(ProducerConfig config)
        {
            if (CurrentConnection?.UseSasl == true)
            {
                config.SecurityProtocol = CurrentConnection.UseSsl
                    ? SecurityProtocol.SaslSsl
                    : SecurityProtocol.SaslPlaintext;
                config.SaslMechanism = CurrentConnection.SaslMechanism switch
                {
                    "PLAIN" => Confluent.Kafka.SaslMechanism.Plain,
                    "SCRAM-SHA-256" => Confluent.Kafka.SaslMechanism.ScramSha256,
                    "SCRAM-SHA-512" => Confluent.Kafka.SaslMechanism.ScramSha512,
                    "GSSAPI" => Confluent.Kafka.SaslMechanism.Gssapi,
                    _ => Confluent.Kafka.SaslMechanism.Plain
                };
                config.SaslUsername = CurrentConnection.SaslUsername;
                config.SaslPassword = CurrentConnection.SaslPassword;
            }
            else if (CurrentConnection?.UseSsl == true)
            {
                config.SecurityProtocol = SecurityProtocol.Ssl;
            }

            ApplySslFileConfig(config);
        }

        private void ApplySaslConfig(ConsumerConfig config)
        {
            if (CurrentConnection?.UseSasl == true)
            {
                config.SecurityProtocol = CurrentConnection.UseSsl
                    ? SecurityProtocol.SaslSsl
                    : SecurityProtocol.SaslPlaintext;
                config.SaslMechanism = CurrentConnection.SaslMechanism switch
                {
                    "PLAIN" => Confluent.Kafka.SaslMechanism.Plain,
                    "SCRAM-SHA-256" => Confluent.Kafka.SaslMechanism.ScramSha256,
                    "SCRAM-SHA-512" => Confluent.Kafka.SaslMechanism.ScramSha512,
                    "GSSAPI" => Confluent.Kafka.SaslMechanism.Gssapi,
                    _ => Confluent.Kafka.SaslMechanism.Plain
                };
                config.SaslUsername = CurrentConnection.SaslUsername;
                config.SaslPassword = CurrentConnection.SaslPassword;
            }
            else if (CurrentConnection?.UseSsl == true)
            {
                config.SecurityProtocol = SecurityProtocol.Ssl;
            }

            ApplySslFileConfig(config);
        }

        #endregion

        #region Producer

        private async Task SendMessageAsync()
        {
            if (string.IsNullOrWhiteSpace(ProducerTopic) || string.IsNullOrWhiteSpace(ProducerMessage))
            {
                ProducerStatus = "请输入 Topic 和消息内容";
                _logger.Warn("发送消息前缺少 Topic 或消息内容。");
                return;
            }

            try
            {
                ProducerStatus = "正在发送...";

                ValidateProducerPayload();
                var headers = ParseProducerHeaders();

                var useKey = !string.IsNullOrWhiteSpace(ProducerKey);

                await _kafkaService.SendMessageAsync(
                    KafkaBootstrapServer,
                    ProducerTopic,
                    useKey ? ProducerKey : null,
                    ProducerMessage,
                    ProducerPartition,
                    headers,
                    cfg => ApplySaslConfig(cfg)
                );

                ProducerStatus = $"✓ 消息发送成功 - {DateTime.Now:HH:mm:ss}";
                _logger.Info($"消息发送成功: topic={ProducerTopic}, partition={(ProducerPartition?.ToString() ?? "auto")}。");
            }
            catch (Exception ex)
            {
                ProducerStatus = $"✗ 发送失败: {ex.Message}";
                _logger.Error($"消息发送失败: topic={ProducerTopic}。", ex);
                MessageBox.Show(ex.Message, "发送失败", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        #endregion

        #region Consumer

        private async Task SubscribeMessageAsync(TextBox tb)
        {
            if (string.IsNullOrWhiteSpace(ConsumerTopic) || string.IsNullOrWhiteSpace(ConsumerGroup))
            {
                ConsumerTooltipMsg = "请输入 Consumer Topic 和 Group";
                _logger.Warn("订阅消息前缺少 Topic 或 Group。");
                return;
            }

            IsConsumerReadonly = true;
            IsConsuming = true;
            ConsumerMessage = "";
            ConsumerErrorMessage = "";

            await Task.Run(() =>
            {
                try
                {
                    var config = new ConsumerConfig
                    {
                        GroupId = ConsumerGroup,
                        BootstrapServers = KafkaBootstrapServer,
                        AutoOffsetReset = AutoOffsetReset.Earliest,
                        EnableAutoCommit = true,
                    };

                    ApplySaslConfig(config);

                    var consumer = new ConsumerBuilder<string, string>(config).Build();
                    consumer.Subscribe(ConsumerTopic);
                    _logger.Info($"开始订阅 Topic: {ConsumerTopic}, Group: {ConsumerGroup}。");

                    int _uiUpdateCounter = 0;
                    while (!_consumerCts.IsCancellationRequested)
                    {
                        try
                        {
                            var cr = consumer.Consume(_consumerCts.Token);
                            var msg = $"[{cr.TopicPartitionOffset}] [{cr.Message.Timestamp.UtcDateTime:HH:mm:ss.fff}] " +
                                 $"Key={cr.Message.Key ?? "(null)"} Value={cr.Message.Value}";

                            // Use BeginInvoke with lower priority to avoid blocking the UI thread
                            Application.Current.Dispatcher.BeginInvoke(DispatcherPriority.Background, new Action(() =>
                            {
                                AppendConsumerMessage(msg);
                                // throttle frequent ScrollToEnd calls to reduce UI work
                                _uiUpdateCounter++;
                                if ((_uiUpdateCounter % 5) == 0)
                                    tb?.ScrollToEnd();
                                ConsumerTooltipMsg = $"已消费: {cr.TopicPartitionOffset}";
                            }));

                            ClearTooltipAfterDelay(5000);
                        }
                        catch (OperationCanceledException)
                        {
                            break;
                        }
                        catch (ConsumeException e)
                        {
                            _logger.Warn($"消费出现可恢复异常: {e.Error.Reason}。");
                            Application.Current.Dispatcher.BeginInvoke(() =>
                            {
                                ConsumerTooltipMsg = $"消费错误: {e.Error.Reason}";
                            });
                            ClearTooltipAfterDelay(5000);
                        }
                    }

                    consumer.Close();
                }
                catch (Exception ex)
                {
                    Application.Current.Dispatcher.Invoke(() =>
                    {
                        IsConsumerReadonly = false;
                        IsConsuming = false;
                        ConsumerErrorMessage = $"消费异常: {ex.Message}";
                        _logger.Error($"消费失败: topic={ConsumerTopic}, group={ConsumerGroup}。", ex);
                        MessageBox.Show(
                            $"消费异常: {ex.Message}\n请检查 Topic、消费者组、网络连接和认证配置。",
                            "消费失败",
                            MessageBoxButton.OK,
                            MessageBoxImage.Error);
                    });
                }
                finally
                {
                    Application.Current.Dispatcher.Invoke(() =>
                    {
                        IsConsumerReadonly = false;
                        IsConsuming = false;
                    });

                }
            });
        }

        private void CancelSubscribe()
        {
            _consumerCts.Cancel();
            _consumerCts = new CancellationTokenSource();
            IsConsumerReadonly = false;
            IsConsuming = false;
            ConsumerMessage = "";
            ConsumerErrorMessage = "";
            ConsumerTooltipMsg = "已取消订阅";
            ClearTooltipAfterDelay(5000);
            _logger.Info($"已取消订阅 Topic: {ConsumerTopic}, Group: {ConsumerGroup}。");
        }

        private void AppendConsumerMessage(string message)
        {
            var next = string.IsNullOrEmpty(ConsumerMessage)
                ? message + "\r\n"
                : ConsumerMessage + message + "\r\n";

            if (next.Length > MaxConsumerMessageChars)
            {
                next = next[^MaxConsumerMessageChars..];
                var firstLineBreak = next.IndexOf('\n');
                if (firstLineBreak >= 0 && firstLineBreak < next.Length - 1)
                {
                    next = next[(firstLineBreak + 1)..];
                }
            }

            ConsumerMessage = next;
        }

        private async void ClearTooltipAfterDelay(int delayMs)
        {
            _clearCts.Cancel();
            _clearCts = new CancellationTokenSource();
            var token = _clearCts.Token;
            try
            {
                await Task.Delay(delayMs, token);
                if (!token.IsCancellationRequested)
                    Application.Current.Dispatcher.Invoke(() => ConsumerTooltipMsg = "");
            }
            catch (TaskCanceledException)
            {
                // cancelled - nothing to do
            }
        }

        #endregion

        #region Topic Management

        private async Task AcquireTopicListAsync()
        {
            try
            {
                var topics = await _kafkaService.GetTopicListAsync(KafkaBootstrapServer, cfg => ApplySaslConfig(cfg));

                string topicNames = JsonSerializer.Serialize(topics.Select(t => new Topic(t.TopicName)).ToList());
                var param = new DialogParameters { { "TopicList", topicNames } };
                _dialogService.ShowDialog("TopicListDialogWindow", param);
            }
            catch (Exception ex)
            {
                _logger.Error("获取 Topic 列表弹窗数据失败。", ex);
                MessageBox.Show($"获取Topic列表失败: {ex.Message}", "错误", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        private async Task RefreshTopicListAsync()
        {
            IsTopicLoading = true;
            TopicStatus = "正在加载Topic列表...";
            try
            {
                var topics = await _kafkaService.GetTopicListAsync(KafkaBootstrapServer, cfg => ApplySaslConfig(cfg));
                TopicList.Clear();
                foreach (var t in topics)
                    TopicList.Add(t);

                FilterTopicList();
                TopicStatus = $"✓ 已加载 {topics.Count} 个Topic";
                _logger.Info($"Topic 列表刷新完成，数量: {topics.Count}。");
            }
            catch (Exception ex)
            {
                TopicStatus = $"✗ 加载失败: {ex.Message}";
                _logger.Error("刷新 Topic 列表失败。", ex);
            }
            finally
            {
                IsTopicLoading = false;
            }
        }

        private void FilterTopicList()
        {
            FilteredTopicList.Clear();
            var filtered = string.IsNullOrWhiteSpace(TopicFilter)
                ? TopicList
                : new ObservableCollection<TopicInfo>(
                    TopicList.Where(t => t.TopicName.Contains(TopicFilter, StringComparison.OrdinalIgnoreCase)));

            foreach (var t in filtered)
                FilteredTopicList.Add(t);
        }

        private async Task CreateTopicAsync()
        {
            var result = await _dialogService.ShowDialogAsync("CreateTopicDialogWindow");

            if (result.Result == ButtonResult.None)
            {
                var action = result.Parameters.GetValue<string>("action");
                if (action == "create")
                {
                    var topicName = result.Parameters.GetValue<string>("topicName");
                    var partitions = result.Parameters.GetValue<int>("partitions");
                    var replicationFactor = result.Parameters.GetValue<short>("replicationFactor");
                    var configs = result.Parameters.GetValue<Dictionary<string, string>>("configs");

                    try
                    {
                        await _kafkaService.CreateTopicAsync(
                            KafkaBootstrapServer, topicName, partitions, replicationFactor, configs,
                            cfg => ApplySaslConfig(cfg));

                        TopicStatus = $"✓ Topic '{topicName}' 创建成功";
                        _logger.Info($"Topic 创建成功: {topicName}, partitions={partitions}, replicationFactor={replicationFactor}。");

                        // Auto refresh
                        await RefreshTopicListAsync();
                    }
                    catch (Exception ex)
                    {
                        TopicStatus = $"✗ 创建失败: {ex.Message}";
                        _logger.Error($"创建 Topic 失败: {topicName}。", ex);
                        MessageBox.Show($"创建Topic失败: {ex.Message}", "错误", MessageBoxButton.OK, MessageBoxImage.Error);
                    }
                }
            }
        }

        private async Task DeleteTopicAsync()
        {
            if (SelectedTopic == null) return;

            var result = MessageBox.Show($"确定要删除Topic '{SelectedTopic.TopicName}' 吗？\n此操作不可恢复！",
                "确认删除", MessageBoxButton.YesNo, MessageBoxImage.Warning);

            if (result == MessageBoxResult.Yes)
            {
                try
                {
                    await _kafkaService.DeleteTopicAsync(KafkaBootstrapServer, SelectedTopic.TopicName, cfg => ApplySaslConfig(cfg));
                    TopicStatus = $"✓ Topic '{SelectedTopic.TopicName}' 已删除";
                    _logger.Info($"Topic 已删除: {SelectedTopic.TopicName}。");
                    await RefreshTopicListAsync();
                }
                catch (Exception ex)
                {
                    TopicStatus = $"✗ 删除失败: {ex.Message}";
                    _logger.Error($"删除 Topic 失败: {SelectedTopic.TopicName}。", ex);
                    MessageBox.Show($"删除Topic失败: {ex.Message}", "错误", MessageBoxButton.OK, MessageBoxImage.Error);
                }
            }
        }

        private async Task ViewTopicDetailAsync()
        {
            if (SelectedTopic == null) return;

            TopicStatus = $"正在加载 Topic '{SelectedTopic.TopicName}' 的详情...";
            try
            {
                TopicDetail = await _kafkaService.GetTopicDetailAsync(
                    KafkaBootstrapServer, SelectedTopic.TopicName, cfg => ApplySaslConfig(cfg));

                if (TopicDetail.Partitions.Count > 0)
                    SelectedPartition = TopicDetail.Partitions[0];
                TopicConfigEntries = string.Join(Environment.NewLine,
                    TopicDetail.Configs.OrderBy(c => c.Key).Select(c => $"{c.Key}={c.Value}"));
                TopicTargetPartitionCount = TopicDetail.PartitionCount;
                TopicStatus = $"✓ 已加载 Topic '{TopicDetail.TopicName}' 的详情";
                _logger.Info($"Topic 详情加载成功: {TopicDetail.TopicName}。");
            }
            catch (Exception ex)
            {
                TopicDetail = null;
                SelectedPartition = null;
                TopicConfigEntries = string.Empty;
                TopicStatus = $"✗ 获取详情失败: {ex.Message}";
                _logger.Error($"加载 Topic 详情失败: {SelectedTopic?.TopicName}。", ex);
            }
        }

        private void FormatProducerJson()
        {
            try
            {
                using var doc = JsonDocument.Parse(ProducerMessage);
                ProducerMessage = JsonSerializer.Serialize(doc.RootElement, new JsonSerializerOptions
                {
                    WriteIndented = true
                });
                ProducerMessageIsJson = true;
                ProducerStatus = "✓ JSON 已格式化";
                _logger.Info("生产消息 JSON 已格式化。");
            }
            catch (Exception ex)
            {
                ProducerStatus = $"✗ JSON 格式化失败: {ex.Message}";
                _logger.Warn($"生产消息 JSON 格式化失败: {ex.Message}");
            }
        }

        private void ValidateProducerPayload()
        {
            if (string.IsNullOrWhiteSpace(ProducerMessage))
            {
                throw new Exception("消息内容不能为空。");
            }

            ProducerMessageIsJson = false;
            var trimmed = ProducerMessage.Trim();
            if (trimmed.StartsWith("{") || trimmed.StartsWith("["))
            {
                using var _ = JsonDocument.Parse(trimmed);
                ProducerMessageIsJson = true;
            }

            _ = ParseProducerHeaders();
            ProducerStatus = ProducerMessageIsJson ? "✓ JSON 与 Headers 校验通过" : "✓ 消息与 Headers 校验通过";
        }

        private void ValidateProducerPayloadFromUi()
        {
            try
            {
                ValidateProducerPayload();
            }
            catch (Exception ex)
            {
                ProducerStatus = $"✗ 校验失败: {ex.Message}";
                _logger.Warn($"生产消息校验失败: {ex.Message}");
                MessageBox.Show($"消息校验失败: {ex.Message}", "校验失败", MessageBoxButton.OK, MessageBoxImage.Warning);
            }
        }

        private Dictionary<string, string> ParseProducerHeaders()
        {
            Dictionary<string, string> headers = null;
            if (!string.IsNullOrWhiteSpace(ProducerHeaders))
            {
                headers = new Dictionary<string, string>();
                foreach (var rawLine in ProducerHeaders.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries))
                {
                    var line = rawLine.Trim();
                    var parts = line.Split('=', 2);
                    if (parts.Length != 2 || string.IsNullOrWhiteSpace(parts[0]))
                    {
                        throw new Exception($"Headers 格式无效: {line}");
                    }
                    headers[parts[0].Trim()] = parts[1].Trim();
                }
            }

            return headers;
        }

        private async Task SaveTopicConfigsAsync()
        {
            if (TopicDetail == null)
            {
                MessageBox.Show("请先加载 Topic 详情。", "提示", MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            try
            {
                var configs = ParseConfigEntries(TopicConfigEntries);
                await _kafkaService.UpdateTopicConfigsAsync(
                    KafkaBootstrapServer,
                    TopicDetail.TopicName,
                    configs,
                    cfg => ApplySaslConfig(cfg));

                TopicStatus = $"✓ Topic '{TopicDetail.TopicName}' 配置已更新";
                _logger.Info($"Topic 配置已更新: {TopicDetail.TopicName}, configCount={configs.Count}。");
                await ViewTopicDetailAsync();
            }
            catch (Exception ex)
            {
                TopicStatus = $"✗ 配置更新失败: {ex.Message}";
                _logger.Error($"更新 Topic 配置失败: {TopicDetail?.TopicName}。", ex);
                MessageBox.Show($"更新 Topic 配置失败: {ex.Message}", "错误", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        private async Task ExpandTopicPartitionsAsync()
        {
            if (TopicDetail == null)
            {
                MessageBox.Show("请先加载 Topic 详情。", "提示", MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            if (TopicTargetPartitionCount <= TopicDetail.PartitionCount)
            {
                MessageBox.Show(
                    $"目标分区数必须大于当前分区数 {TopicDetail.PartitionCount}。",
                    "提示",
                    MessageBoxButton.OK,
                    MessageBoxImage.Information);
                return;
            }

            var result = MessageBox.Show(
                $"确定将 Topic '{TopicDetail.TopicName}' 的分区数扩容到 {TopicTargetPartitionCount} 吗？",
                "确认扩容",
                MessageBoxButton.YesNo,
                MessageBoxImage.Question);

            if (result != MessageBoxResult.Yes)
            {
                return;
            }

            try
            {
                await _kafkaService.IncreaseTopicPartitionsAsync(
                    KafkaBootstrapServer,
                    TopicDetail.TopicName,
                    TopicTargetPartitionCount,
                    cfg => ApplySaslConfig(cfg));

                TopicStatus = $"✓ Topic '{TopicDetail.TopicName}' 已扩容到 {TopicTargetPartitionCount} 个分区";
                _logger.Info($"Topic 已扩容: {TopicDetail.TopicName}, targetPartitions={TopicTargetPartitionCount}。");
                await RefreshTopicListAsync();
                await ViewTopicDetailAsync();
            }
            catch (Exception ex)
            {
                TopicStatus = $"✗ 分区扩容失败: {ex.Message}";
                _logger.Error($"扩容 Topic 分区失败: {TopicDetail?.TopicName}。", ex);
                MessageBox.Show($"扩容 Topic 分区失败: {ex.Message}", "错误", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        private static Dictionary<string, string> ParseConfigEntries(string configEntries)
        {
            var configDict = new Dictionary<string, string>();
            if (string.IsNullOrWhiteSpace(configEntries))
            {
                return configDict;
            }

            foreach (var rawLine in configEntries.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries))
            {
                var line = rawLine.Trim();
                if (string.IsNullOrWhiteSpace(line))
                {
                    continue;
                }

                var parts = line.Split('=', 2);
                if (parts.Length != 2 || string.IsNullOrWhiteSpace(parts[0]))
                {
                    throw new Exception($"配置项格式无效: {line}");
                }

                configDict[parts[0].Trim()] = parts[1].Trim();
            }

            return configDict;
        }

        #endregion

        #region Consumer Groups

        private async Task RefreshConsumerGroupsAsync()
        {
            ConsumerGroupStatus = "正在加载消费者组...";
            try
            {
                var groups = await _kafkaService.GetConsumerGroupsAsync(KafkaBootstrapServer, cfg => ApplySaslConfig(cfg));
                ConsumerGroups.Clear();
                foreach (var g in groups.Where(gr => gr.ProtocolType == "consumer"))
                    ConsumerGroups.Add(g);

                ConsumerGroupStatus = $"✓ 已加载 {ConsumerGroups.Count} 个消费者组";
                _logger.Info($"消费者组列表刷新完成，数量: {ConsumerGroups.Count}。");
            }
            catch (Exception ex)
            {
                ConsumerGroupStatus = $"✗ 加载失败: {ex.Message}";
                _logger.Error("刷新消费者组列表失败。", ex);
            }
        }

        private async Task ViewConsumerGroupDetailAsync()
        {
            if (SelectedConsumerGroup == null) return;

            ConsumerGroupStatus = $"正在加载 '{SelectedConsumerGroup.GroupId}' 的详情...";
            try
            {
                var details = await _kafkaService.GetConsumerGroupDetailsAsync(
                    KafkaBootstrapServer, SelectedConsumerGroup.GroupId, cfg => ApplySaslConfig(cfg));

                ConsumerGroupDetails.Clear();
                foreach (var d in details)
                    ConsumerGroupDetails.Add(d);

                TotalLag = details.Sum(d => d.Lag);
                ConsumerGroupStatus = $"✓ '{SelectedConsumerGroup.GroupId}' - {details.Count} 个分区, 总Lag: {TotalLag}";
                _logger.Info($"消费者组详情加载成功: {SelectedConsumerGroup.GroupId}, partitions={details.Count}, totalLag={TotalLag}。");

                if (details.Count == 0)
                {
                    MessageBox.Show("该消费者组没有已提交的偏移量信息。\n可能的原因：\n1. 消费者组尚未消费过该Topic\n2. 使用了自动提交但未提交过\n3. 消费者组已过期",
                        "提示", MessageBoxButton.OK, MessageBoxImage.Information);
                }
            }
            catch (Exception ex)
            {
                ConsumerGroupStatus = $"✗ 加载详情失败: {ex.Message}";
                _logger.Error($"加载消费者组详情失败: {SelectedConsumerGroup?.GroupId}。", ex);
            }
        }

        private async Task DeleteConsumerGroupAsync()
        {
            if (SelectedConsumerGroup == null) return;

            var result = MessageBox.Show($"确定要删除消费者组 '{SelectedConsumerGroup.GroupId}' 吗？",
                "确认删除", MessageBoxButton.YesNo, MessageBoxImage.Warning);

            if (result == MessageBoxResult.Yes)
            {
                try
                {
                    await _kafkaService.DeleteConsumerGroupAsync(
                        KafkaBootstrapServer, SelectedConsumerGroup.GroupId, cfg => ApplySaslConfig(cfg));
                    ConsumerGroupStatus = $"✓ 消费者组 '{SelectedConsumerGroup.GroupId}' 已删除";
                    _logger.Info($"消费者组已删除: {SelectedConsumerGroup.GroupId}。");
                    await RefreshConsumerGroupsAsync();
                }
                catch (Exception ex)
                {
                    ConsumerGroupStatus = $"✗ 删除失败: {ex.Message}";
                    _logger.Error($"删除消费者组失败: {SelectedConsumerGroup?.GroupId}。", ex);
                    MessageBox.Show($"删除消费者组失败: {ex.Message}", "错误", MessageBoxButton.OK, MessageBoxImage.Error);
                }
            }
        }

        private async Task ResetConsumerGroupOffsetAsync()
        {
            if (SelectedConsumerGroup == null || ConsumerGroupDetails.Count == 0) return;

            var preview = BuildResetOffsetPreview();
            var result = MessageBox.Show(
                preview,
                "确认重置偏移量",
                MessageBoxButton.YesNo,
                MessageBoxImage.Warning);

            if (result == MessageBoxResult.No) return;

            var resetTimestamp = ResolveDateTimeInput(ResetOffsetTimestamp, ResetOffsetTimeText, "偏移量重置时间");
            ResetOffsetTimestamp = resetTimestamp;
            ResetOffsetTimeText = resetTimestamp.ToString("HH:mm:ss");

            ConsumerGroupStatus = "正在重置偏移量...";
            try
            {
                await _kafkaService.ResetConsumerGroupOffsetsAsync(
                    KafkaBootstrapServer,
                    SelectedConsumerGroup.GroupId,
                    ConsumerGroupDetails.ToList(),
                    ResetOffsetStrategy,
                    ResetOffsetValue,
                    resetTimestamp,
                    cfg => ApplySaslConfig(cfg));

                ConsumerGroupStatus = $"✓ 偏移量重置成功，请重新加载查看详情";
                ConsumerGroupDetails.Clear();
                TotalLag = 0;
                _logger.Info($"消费者组偏移量重置成功: {SelectedConsumerGroup.GroupId}, strategy={ResetOffsetStrategy}。");
            }
            catch (Exception ex)
            {
                ConsumerGroupStatus = $"✗ 重置失败: {ex.Message}";
                _logger.Error($"重置消费者组偏移量失败: {SelectedConsumerGroup?.GroupId}, strategy={ResetOffsetStrategy}。", ex);
                MessageBox.Show($"重置偏移量失败: {ex.Message}", "错误", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        #endregion

        #region Cluster

        private async Task RefreshClusterInfoAsync()
        {
            ClusterStatus = "正在获取集群信息...";
            try
            {
                ClusterInfo = await _kafkaService.GetClusterInfoAsync(KafkaBootstrapServer, cfg => ApplySaslConfig(cfg));
                ClusterStatus = $"✓ Cluster: {ClusterInfo.ClusterId}, Brokers: {ClusterInfo.BrokerCount}, Topics: {ClusterInfo.TopicCount}";
                _logger.Info($"集群信息刷新完成: cluster={ClusterInfo.ClusterId}, brokers={ClusterInfo.BrokerCount}, topics={ClusterInfo.TopicCount}。");
            }
            catch (Exception ex)
            {
                ClusterStatus = $"✗ 获取失败: {ex.Message}";
                _logger.Error("刷新集群信息失败。", ex);
            }
        }

        #endregion

        #region Message Browser

        private async Task BrowseMessagesAsync()
        {
            if (string.IsNullOrWhiteSpace(BrowseTopic))
            {
                MessageBox.Show("请输入要浏览的 Topic 名称", "提示", MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            try
            {
                List<MessageRecord> messages;
                var browseTimestamp = ResolveDateTimeInput(BrowseTimestamp, BrowseTimeText, "消息浏览时间");
                BrowseTimestamp = browseTimestamp;
                BrowseTimeText = browseTimestamp.ToString("HH:mm:ss");

                if (BrowseByTimestamp)
                {
                    messages = await _kafkaService.ConsumeFromTimestampAsync(
                        KafkaBootstrapServer, BrowseTopic, browseTimestamp, BrowseCount, BrowseGroupId, cfg =>
                        {
                            cfg.AutoOffsetReset = AutoOffsetReset.Earliest;
                            ApplySaslConfig(cfg);
                        });
                }
                else
                {
                    messages = await _kafkaService.BrowseMessagesAsync(
                        KafkaBootstrapServer, BrowseTopic, BrowsePartition, BrowseOffset, BrowseCount, BrowseGroupId, cfg =>
                        {
                            cfg.AutoOffsetReset = AutoOffsetReset.Earliest;
                            ApplySaslConfig(cfg);
                        });
                }

                if (messages.Count == 0)
                {
                    _logger.Info($"消息浏览未命中数据: topic={BrowseTopic}, partition={(BrowsePartition?.ToString() ?? "all")}。");
                    MessageBox.Show("未找到消息", "提示", MessageBoxButton.OK, MessageBoxImage.Information);
                    return;
                }

                var param = new DialogParameters
                {
                    { "messages", messages },
                    { "topic", BrowseTopic }
                };

                _lastBrowsedMessages = messages;
                _lastBrowseTopic = BrowseTopic;
                _lastBrowsePartition = BrowsePartition;
                if (_lastBrowsePartition == null)
                {
                    var partitions = messages.Select(m => m.Partition).Distinct().ToList();
                    _lastBrowsePartition = partitions.Count == 1 ? partitions[0] : null;
                }

                var result = await _dialogService.ShowDialogAsync("BrowseMessageDialogWindow", param);
                ApplyBrowseDialogResult(result);
                _logger.Info($"消息浏览成功: topic={BrowseTopic}, count={messages.Count}, byTimestamp={BrowseByTimestamp}。");
            }
            catch (Exception ex)
            {
                _logger.Error($"消息浏览失败: topic={BrowseTopic}。", ex);
                MessageBox.Show($"浏览消息失败: {ex.Message}", "错误", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        private async Task ContinueBrowseMessagesAsync()
        {
            if (_lastBrowsedMessages == null || _lastBrowsedMessages.Count == 0 || string.IsNullOrWhiteSpace(_lastBrowseTopic))
            {
                MessageBox.Show("请先执行一次消息浏览。", "提示", MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            if (_lastBrowsePartition == null)
            {
                MessageBox.Show("当前浏览结果包含多个分区，请指定分区后再继续读取。", "提示", MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            var lastOffset = _lastBrowsedMessages
                .Where(m => m.Partition == _lastBrowsePartition)
                .Max(m => m.Offset);

            BrowseTopic = _lastBrowseTopic;
            BrowsePartition = _lastBrowsePartition;
            BrowseOffset = lastOffset + 1;
            BrowseByTimestamp = false;

            await BrowseMessagesAsync();
        }

        private void ApplyBrowseDialogResult(IDialogResult result)
        {
            if (result.Result != ButtonResult.None && result.Result != ButtonResult.OK)
            {
                return;
            }

            var action = result.Parameters.GetValue<string>("action");
            if (action != "copyToProducer")
            {
                return;
            }

            var message = result.Parameters.GetValue<MessageRecord>("message");
            if (message == null)
            {
                return;
            }

            ProducerTopic = message.Topic;
            ProducerPartition = message.Partition;
            ProducerKey = message.Key;
            ProducerMessage = message.Value;
            ProducerHeaders = string.Join(Environment.NewLine, message.Headers.Select(h => $"{h.Key}={h.Value}"));
            SelectedTabIndex = 0;
            ProducerStatus = "✓ 已从浏览结果载入生产模板";
            _logger.Info($"已将浏览消息复制为生产模板: topic={message.Topic}, partition={message.Partition}, offset={message.Offset}。");
        }

        private string BuildResetOffsetPreview()
        {
            var strategyText = ResetOffsetStrategy switch
            {
                "earliest" => "最早可用偏移",
                "latest" => "最新偏移",
                "specific" => $"指定偏移 {ResetOffsetValue ?? 0}",
                "timestamp" => $"时间 {ResolveDateTimeInput(ResetOffsetTimestamp, ResetOffsetTimeText, "偏移量重置时间"):yyyy-MM-dd HH:mm:ss}",
                _ => ResetOffsetStrategy
            };

            var sampleLines = ConsumerGroupDetails
                .Take(5)
                .Select(d => $"  {d.TopicPartitionText}: 当前 {d.CurrentOffset}, Lag {d.Lag}, Owner {d.OwnerText}");

            return
                $"将对消费者组 '{SelectedConsumerGroup.GroupId}' 执行偏移量重置。\n" +
                $"策略: {strategyText}\n" +
                $"分区数: {ConsumerGroupDetails.Count}\n" +
                $"涉及 Topic: {string.Join(", ", ConsumerGroupDetails.Select(d => d.Topic).Distinct())}\n\n" +
                $"预览（前 5 个分区）:\n{string.Join("\n", sampleLines)}\n\n" +
                "确认后将立即提交新的消费位点。";
        }

        #endregion

        #region Window

        private void WindowClosing()
        {
            _consumerCts.Cancel();
            _logger.Info("主窗口关闭，已请求停止后台消费。");
        }

        private static DateTime ResolveDateTimeInput(DateTime date, string timeText, string fieldName)
        {
            if (string.IsNullOrWhiteSpace(timeText))
            {
                return date.Date;
            }

            if (TimeSpan.TryParse(timeText, out var time))
            {
                return date.Date.Add(time);
            }

            throw new Exception($"{fieldName}格式无效，请输入 HH:mm 或 HH:mm:ss。");
        }

        #endregion
    }
}
