using Confluent.Kafka;
using KafkaToolWpf.Helpers;
using KafkaToolWpf.Models;
using KafkaToolWpf.Services;
using Prism.Commands;
using Prism.Dialogs;
using Prism.Mvvm;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
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
        private readonly IKafkaService _kafkaService;
        private readonly IDialogService _dialogService;

        private CancellationTokenSource _consumerCts;
        private CancellationTokenSource _clearCts;
        private SysTimer _tooltipTimer;

        #region Connection

        private string _kafkaBootstrapServer = "192.168.1.100:9092";
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

        #endregion

        #region Commands

        public ICommand SendMessageCommand { get; }
        public ICommand SubscribeMessageCommand { get; }
        public ICommand CancelSubscribeCommand { get; }
        public ICommand WindowClosingCommand { get; }
        public ICommand AcquireTopicListCommand { get; }
        public ICommand RefreshTopicListCommand { get; }
        public ICommand CreateTopicCommand { get; }
        public ICommand DeleteTopicCommand { get; }
        public ICommand ViewTopicDetailCommand { get; }
        public ICommand RefreshConsumerGroupsCommand { get; }
        public ICommand ViewConsumerGroupDetailCommand { get; }
        public ICommand DeleteConsumerGroupCommand { get; }
        public ICommand ResetConsumerGroupOffsetCommand { get; }
        public ICommand RefreshClusterInfoCommand { get; }
        public ICommand BrowseMessagesCommand { get; }
        public ICommand TestConnectionCommand { get; }
        public ICommand ManageConnectionsCommand { get; }
        public ICommand SelectConnectionCommand { get; }

        #endregion

        public MainWindowViewModel(IKafkaService kafkaService, IDialogService dialogService)
        {
            _kafkaService = kafkaService;
            _dialogService = dialogService;

            _consumerCts = new CancellationTokenSource();
            _clearCts = new CancellationTokenSource();
            IsConsumerReadonly = false;
            IsConsuming = false;

            // Init default connections
            Connections.Add(new ConnectionConfig
            {
                Name = "默认集群",
                BootstrapServers = "192.168.1.100:9092"
            });
            CurrentConnection = Connections[0];

            // Init commands
            SendMessageCommand = new AsyncDelegateCommand(SendMessageAsync);
            SubscribeMessageCommand = new AsyncDelegateCommand<TextBox>(SubscribeMessageAsync);
            CancelSubscribeCommand = new DelegateCommand(CancelSubscribe);
            WindowClosingCommand = new DelegateCommand(WindowClosing);
            AcquireTopicListCommand = new AsyncDelegateCommand(AcquireTopicListAsync);
            RefreshTopicListCommand = new AsyncDelegateCommand(RefreshTopicListAsync);
            CreateTopicCommand = new AsyncDelegateCommand(CreateTopicAsync);
            DeleteTopicCommand = new AsyncDelegateCommand(DeleteTopicAsync);
            ViewTopicDetailCommand = new AsyncDelegateCommand(ViewTopicDetailAsync);
            RefreshConsumerGroupsCommand = new AsyncDelegateCommand(RefreshConsumerGroupsAsync);
            ViewConsumerGroupDetailCommand = new AsyncDelegateCommand(ViewConsumerGroupDetailAsync);
            DeleteConsumerGroupCommand = new AsyncDelegateCommand(DeleteConsumerGroupAsync);
            ResetConsumerGroupOffsetCommand = new AsyncDelegateCommand(ResetConsumerGroupOffsetAsync);
            RefreshClusterInfoCommand = new AsyncDelegateCommand(RefreshClusterInfoAsync);
            BrowseMessagesCommand = new AsyncDelegateCommand(BrowseMessagesAsync);
            TestConnectionCommand = new AsyncDelegateCommand(TestConnectionAsync);
            ManageConnectionsCommand = new AsyncDelegateCommand(ManageConnectionsAsync);
            SelectConnectionCommand = new DelegateCommand<ConnectionConfig>(SelectConnection);

            // Tooltip timer
            _tooltipTimer = new SysTimer(5000);
            _tooltipTimer.Elapsed += (s, e) => ConsumerTooltipMsg = "";
        }

        #region Connection Management

        private void SelectConnection(ConnectionConfig connection)
        {
            if (connection != null)
            {
                CurrentConnection = connection;
                ConnectionStatus = $"已连接: {connection.Name} ({connection.BootstrapServers})";
            }
        }

        private async Task ManageConnectionsAsync()
        {
            var param = new DialogParameters
            {
                { "connections", Connections }
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
            }
            catch (Exception ex)
            {
                ConnectionStatus = $"✗ 连接失败: {ex.Message}";
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
        }

        #endregion

        #region Producer

        private async Task SendMessageAsync()
        {
            if (string.IsNullOrWhiteSpace(ProducerTopic) || string.IsNullOrWhiteSpace(ProducerMessage))
            {
                ProducerStatus = "请输入 Topic 和消息内容";
                return;
            }

            try
            {
                ProducerStatus = "正在发送...";

                Dictionary<string, string> headers = null;
                if (!string.IsNullOrWhiteSpace(ProducerHeaders))
                {
                    headers = new Dictionary<string, string>();
                    foreach (var line in ProducerHeaders.Split('\n', StringSplitOptions.RemoveEmptyEntries))
                    {
                        var parts = line.Split('=', 2);
                        if (parts.Length == 2)
                            headers[parts[0].Trim()] = parts[1].Trim();
                    }
                }

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
            }
            catch (Exception ex)
            {
                ProducerStatus = $"✗ 发送失败: {ex.Message}";
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
                                ConsumerMessage += msg + "\r\n";
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
                        MessageBox.Show(ex.StackTrace);
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
            }
            catch (Exception ex)
            {
                TopicStatus = $"✗ 加载失败: {ex.Message}";
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

                        // Auto refresh
                        await RefreshTopicListAsync();
                    }
                    catch (Exception ex)
                    {
                        TopicStatus = $"✗ 创建失败: {ex.Message}";
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
                    await RefreshTopicListAsync();
                }
                catch (Exception ex)
                {
                    TopicStatus = $"✗ 删除失败: {ex.Message}";
                    MessageBox.Show($"删除Topic失败: {ex.Message}", "错误", MessageBoxButton.OK, MessageBoxImage.Error);
                }
            }
        }

        private async Task ViewTopicDetailAsync()
        {
            if (SelectedTopic == null) return;

            try
            {
                TopicDetail = await _kafkaService.GetTopicDetailAsync(
                    KafkaBootstrapServer, SelectedTopic.TopicName, cfg => ApplySaslConfig(cfg));

                if (TopicDetail.Partitions.Count > 0)
                    SelectedPartition = TopicDetail.Partitions[0];

                // Switch to the first tab showing details (we'll show in a dialog for now)
                var detailText = $"Topic: {TopicDetail.TopicName}\n" +
                                 $"分区数: {TopicDetail.PartitionCount}\n" +
                                 $"副本因子: {TopicDetail.ReplicationFactor}\n\n" +
                                 "分区详情:\n" +
                                 string.Join("\n", TopicDetail.Partitions.Select(p =>
                                     $"  分区 {p.PartitionId}: Leader={p.Leader}, " +
                                     $"副本=[{string.Join(",", p.Replicas)}], " +
                                     $"ISR=[{string.Join(",", p.InSyncReplicas)}], " +
                                     $"偏移: {p.EarliestOffset ?? 0} - {p.LatestOffset ?? 0} " +
                                     $"({p.MessageCount} 条消息)"));

                if (TopicDetail.Configs.Count > 0)
                {
                    detailText += "\n\n配置:\n" +
                        string.Join("\n", TopicDetail.Configs.Select(c => $"  {c.Key} = {c.Value}"));
                }

                MessageBox.Show(detailText, $"Topic 详情 - {TopicDetail.TopicName}",
                    MessageBoxButton.OK, MessageBoxImage.Information);
            }
            catch (Exception ex)
            {
                TopicStatus = $"✗ 获取详情失败: {ex.Message}";
            }
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
            }
            catch (Exception ex)
            {
                ConsumerGroupStatus = $"✗ 加载失败: {ex.Message}";
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

                if (details.Count == 0)
                {
                    MessageBox.Show("该消费者组没有已提交的偏移量信息。\n可能的原因：\n1. 消费者组尚未消费过该Topic\n2. 使用了自动提交但未提交过\n3. 消费者组已过期",
                        "提示", MessageBoxButton.OK, MessageBoxImage.Information);
                }
            }
            catch (Exception ex)
            {
                ConsumerGroupStatus = $"✗ 加载详情失败: {ex.Message}";
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
                    await RefreshConsumerGroupsAsync();
                }
                catch (Exception ex)
                {
                    ConsumerGroupStatus = $"✗ 删除失败: {ex.Message}";
                    MessageBox.Show($"删除消费者组失败: {ex.Message}", "错误", MessageBoxButton.OK, MessageBoxImage.Error);
                }
            }
        }

        private async Task ResetConsumerGroupOffsetAsync()
        {
            if (SelectedConsumerGroup == null || ConsumerGroupDetails.Count == 0) return;

            var result = MessageBox.Show(
                $"确定要重置消费者组 '{SelectedConsumerGroup.GroupId}' 的所有分区偏移量为 0 吗？\n这将导致消费者重新消费所有消息。",
                "确认重置偏移量", MessageBoxButton.YesNo, MessageBoxImage.Warning);

            if (result == MessageBoxResult.No) return;

            ConsumerGroupStatus = "正在重置偏移量...";
            try
            {
                // Get unique topics from details
                var topics = ConsumerGroupDetails.Select(d => d.Topic).Distinct();
                foreach (var topic in topics)
                {
                    await _kafkaService.ResetConsumerGroupOffsetAsync(
                        KafkaBootstrapServer, SelectedConsumerGroup.GroupId, topic, 0, cfg => ApplySaslConfig(cfg));
                }

                ConsumerGroupStatus = $"✓ 偏移量重置成功，请重新加载查看详情";
                ConsumerGroupDetails.Clear();
                TotalLag = 0;
            }
            catch (Exception ex)
            {
                ConsumerGroupStatus = $"✗ 重置失败: {ex.Message}";
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
            }
            catch (Exception ex)
            {
                ClusterStatus = $"✗ 获取失败: {ex.Message}";
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

                if (BrowseByTimestamp)
                {
                    messages = await _kafkaService.ConsumeFromTimestampAsync(
                        KafkaBootstrapServer, BrowseTopic, BrowseTimestamp, BrowseCount, null, cfg =>
                        {
                            cfg.AutoOffsetReset = AutoOffsetReset.Earliest;
                            ApplySaslConfig(cfg);
                        });
                }
                else
                {
                    messages = await _kafkaService.BrowseMessagesAsync(
                        KafkaBootstrapServer, BrowseTopic, BrowsePartition, BrowseOffset, BrowseCount, null, cfg =>
                        {
                            cfg.AutoOffsetReset = AutoOffsetReset.Earliest;
                            ApplySaslConfig(cfg);
                        });
                }

                if (messages.Count == 0)
                {
                    MessageBox.Show("未找到消息", "提示", MessageBoxButton.OK, MessageBoxImage.Information);
                    return;
                }

                var param = new DialogParameters
                {
                    { "messages", messages },
                    { "topic", BrowseTopic }
                };

                _dialogService.ShowDialog("BrowseMessageDialogWindow", param);
            }
            catch (Exception ex)
            {
                MessageBox.Show($"浏览消息失败: {ex.Message}", "错误", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        #endregion

        #region Window

        private void WindowClosing()
        {
            _consumerCts.Cancel();
        }

        #endregion
    }
}