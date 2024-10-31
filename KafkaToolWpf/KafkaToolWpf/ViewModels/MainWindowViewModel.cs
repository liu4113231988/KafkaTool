using Confluent.Kafka;
using KafkaToolWpf.Models;
using KafkaToolWpf.Views;
using Prism.Commands;
using Prism.Dialogs;
using Prism.Mvvm;
using System;
using System.Collections.Generic;
using System.ComponentModel;
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
        private string _title = "KafkaTool";
        public string Title
        {
            get { return _title; }
            set { SetProperty(ref _title, value); }
        }

        private string _kafkaBootstrapServer;

        public string KafkaBootstrapServer
        {
            get { return _kafkaBootstrapServer; }
            set { SetProperty(ref _kafkaBootstrapServer, value); }
        }

        private string _producerTopic;

        public string ProducerTopic
        {
            get { return _producerTopic; }
            set { SetProperty(ref _producerTopic, value); }
        }

        private string _consumerTopic;

        public string ConsumerTopic
        {
            get { return _consumerTopic; }
            set { SetProperty(ref _consumerTopic, value); }
        }

        private string _consumerGroup;

        public string ConsumerGroup
        {
            get { return _consumerGroup; }
            set { SetProperty(ref _consumerGroup, value); }
        }

        private string _producerMessage;

        public string ProducerMessage
        {
            get { return _producerMessage; }
            set { SetProperty(ref _producerMessage, value); }
        }

        private string _consumerMessage;

        public string ConsumerMessage
        {
            get { return _consumerMessage; }
            set
            {
                SetProperty(ref _consumerMessage, value);
            }
        }

        private string _consumerTooltipMsg;
        public string ConsumerTooltipMsg
        {
            get { return _consumerTooltipMsg; }
            set { SetProperty(ref _consumerTooltipMsg, value); }
        }

        private bool _isReadonly;

        public bool IsReadonly
        {
            get { return _isReadonly; }
            set { SetProperty(ref _isReadonly, value); }
        }


        private ProducerConfig producerConfig;

        private ConsumerConfig consumerConfig;

        CancellationTokenSource cts, clearCts;

        public ICommand SendMessageCommand { get; set; }
        public ICommand SubcribeMessageCommand { get; set; }
        public ICommand CancelSubcribeCommand { get; set; }

        public ICommand WindowClosingCommand { get; set; }

        public ICommand AcquireTopicListCommand { get; set; }

        SysTimer Timer { get; set; }
        IDialogService _dialogService;

        public MainWindowViewModel(IDialogService dialogService)
        {
            _kafkaBootstrapServer = "192.168.1.100:9092";
            _dialogService = dialogService;
            cts = new CancellationTokenSource();
            clearCts = new CancellationTokenSource();
            IsReadonly = false;
            SendMessageCommand = new AsyncDelegateCommand(SendMessage);
            SubcribeMessageCommand = new AsyncDelegateCommand<TextBox>(SubcribeMessage, (tb) => !IsReadonly);
            CancelSubcribeCommand = new DelegateCommand(CancelSubcribe);
            WindowClosingCommand = new DelegateCommand(WindowClosing);
            AcquireTopicListCommand = new DelegateCommand(GetTopicList);
            Timer = new SysTimer(5000);
            Timer.Elapsed += Timer_Elapsed;
        }

        private void Timer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            //throw new NotImplementedException();
            ConsumerTooltipMsg = "";
        }

        void WindowClosing()
        {
            cts.Cancel();
        }

        async Task SendMessage()
        {
            if (string.IsNullOrEmpty(ProducerTopic) || string.IsNullOrWhiteSpace(KafkaBootstrapServer)
                || string.IsNullOrWhiteSpace(ProducerMessage))
            {
                return;
            }
            producerConfig = new ProducerConfig { BootstrapServers = KafkaBootstrapServer, MessageTimeoutMs = 10 * 1000 };
            try
            {
                using (var kafkaProducer = new ProducerBuilder<Null, string>(producerConfig).Build())
                {
                    var result = await kafkaProducer.ProduceAsync(ProducerTopic, new Message<Null, string> { Value = ProducerMessage });
                    if (result.Status == PersistenceStatus.NotPersisted)
                    {
                        MessageBox.Show(result.Message.Value);
                    }
                }
            }
            catch (ProduceException<string, string> ex)
            {
                MessageBox.Show(ex.Message);
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }

        async Task SubcribeMessage(TextBox tb)
        {
            if (string.IsNullOrWhiteSpace(ConsumerTopic) || string.IsNullOrWhiteSpace(KafkaBootstrapServer)
                || string.IsNullOrWhiteSpace(ConsumerGroup))
            {
                return;
            }
            IsReadonly = true;
            await Task.Factory.StartNew(() =>
            {
                var conf = new ConsumerConfig
                {
                    GroupId = ConsumerGroup,
                    BootstrapServers = KafkaBootstrapServer,
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                    ClientId = ConsumerGroup
                };

                using (var consumer = new ConsumerBuilder<Ignore, string>(conf).Build())
                {
                    try
                    {
                        consumer.Subscribe(ConsumerTopic);
                        while (!cts.IsCancellationRequested)
                        {
                            try
                            {
                                var cr = consumer.Consume(cts.Token);
                                ConsumerMessage += cr.Message.Value + "\r\n";
                                Application.Current.Dispatcher.Invoke(DispatcherPriority.Normal, () => { tb.ScrollToEnd(); });

                                ConsumerTooltipMsg = "Consumed Success";
                                Console.WriteLine($"Consumed message '{cr.Message}' at: '{cr.TopicPartitionOffset}'.");
                                ClearTooltip();
                            }
                            catch (ConsumeException e)
                            {
                                ConsumerTooltipMsg = $"Consumed failed,error:{e.Error.Reason}";
                                Console.WriteLine($"Error occured: {e.Error.Reason}");
                                ClearTooltip();
                            }
                        }
                    }
                    catch (OperationCanceledException ex)
                    {
                        consumer.Close();
                    }
                    catch (Exception ex)
                    {
                        IsReadonly = false;
                        MessageBox.Show(ex.Message);
                    }
                }
            });

        }

        void GetTopicList()
        {
            var config = new ClientConfig
            {
                BootstrapServers = KafkaBootstrapServer
            };

            using (var adminClient = new AdminClientBuilder(config).Build())
            {
                try
                {
                    var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
                    var topics = metadata.Topics;
                    List<Topic> topicList = topics.Select(t => new Topic(t.Topic)).ToList();
                    string topicNames = JsonSerializer.Serialize(topicList);
                    IDialogParameters param = new DialogParameters
                    {
                        { "TopicList", topicNames }
                    };
                    _dialogService.ShowDialog("TopicListDialogWindow", param);
                }
                catch (Exception e)
                {
                    MessageBox.Show($"Error: {e.Message}");
                    //Console.WriteLine($"Error: {e.Message}");
                }
            }
        }

        void CancelSubcribe()
        {
            IsReadonly = false;
            cts.Cancel();
            cts = new CancellationTokenSource();
            ConsumerMessage = "";
            ConsumerTooltipMsg = "Consumer canceled";
            ClearTooltip();

        }

        void ClearTooltip()
        {

            clearCts.Cancel();
            // 创建新的 CancellationTokenSource
            clearCts = new CancellationTokenSource();
            // 启动新的清除任务
            Task.Delay(TimeSpan.FromSeconds(5), clearCts.Token)
                .ContinueWith(t =>
                {
                    if (!t.IsCanceled)
                    {
                        ClearMessage();
                    }
                });
        }

        private void ClearMessage()
        {
            ConsumerTooltipMsg = string.Empty;
            Console.WriteLine("Message cleared.");
        }
    }
}
