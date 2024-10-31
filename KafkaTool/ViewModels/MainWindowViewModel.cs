using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using Confluent.Kafka;
using MsBox.Avalonia.Enums;
using MsBox.Avalonia;
using System;
using System.Threading.Tasks;
using Tmds.DBus.Protocol;
using System.Threading;
using Avalonia.Controls.ApplicationLifetimes;

namespace KafkaTool.ViewModels
{
    public partial class MainWindowViewModel : ViewModelBase
    {
#pragma warning disable CA1822 // Mark members as static
        public string Greeting => "Welcome to Avalonia!";
#pragma warning restore CA1822 // Mark members as static

        [ObservableProperty]
        private string _kafkaBootstrapServer;

        [ObservableProperty]
        private string _producerTopic;

        [ObservableProperty]
        private string _consumerTopic;

        [ObservableProperty]
        private string _consumerGroup;

        [ObservableProperty]
        private string _producerMessage;

        [ObservableProperty]
        private string _consumerMessage;

        [ObservableProperty]
        private string _selectedCursor;


        [ObservableProperty]
        private bool isReadonly;

        private ProducerConfig producerConfig;

        private ConsumerConfig consumerConfig;

        CancellationTokenSource cts;

        public MainWindowViewModel()
        {
            _kafkaBootstrapServer = "172.17.30.90:9092";
            cts = new CancellationTokenSource();
            IsReadonly = true;
        }

        [RelayCommand]
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
                        await MessageBoxManager.GetMessageBoxStandard("错误", result.Message.Value, ButtonEnum.Ok).ShowWindowDialogAsync(App.MainWindow);
                    }
                }
            }
            catch (ProduceException<string, string> ex)
            {
                await MessageBoxManager.GetMessageBoxStandard("错误", ex.Message, ButtonEnum.Ok).ShowWindowDialogAsync(App.MainWindow);
            }
            catch (Exception ex)
            {
                await MessageBoxManager.GetMessageBoxStandard("错误", ex.Message, ButtonEnum.Ok).ShowWindowDialogAsync(App.MainWindow);
            }
        }

        [RelayCommand]
        async Task SubcribeMessage()
        {
            if (string.IsNullOrWhiteSpace(ConsumerTopic) || string.IsNullOrWhiteSpace(KafkaBootstrapServer)
                || string.IsNullOrWhiteSpace(ConsumerGroup))
            {
                return;
            }
            IsReadonly = false;
            await Task.Factory.StartNew(async () =>
              {
                  var conf = new ConsumerConfig
                  {
                      GroupId = ConsumerGroup,
                      BootstrapServers = KafkaBootstrapServer,
                      AutoOffsetReset = AutoOffsetReset.Earliest,
                      //MaxPollIntervalMs = 60 * 1000,
                  };

                  using (var consumer = new ConsumerBuilder<Ignore, string>(conf).Build())
                  {
                      consumer.Subscribe(ConsumerTopic);

                      try
                      {
                          while (!cts.IsCancellationRequested)
                          {
                              try
                              {
                                  var cr = consumer.Consume();
                                  ConsumerMessage += cr.Message.Value + "\r\n";
                                  Console.WriteLine($"Consumed message '{cr.Message}' at: '{cr.TopicPartitionOffset}'.");
                              }
                              catch (ConsumeException e)
                              {
                                  Console.WriteLine($"Error occured: {e.Error.Reason}");
                              }
                          }
                      }
                      catch (OperationCanceledException ex)
                      {
                          consumer.Close();
                          IsReadonly = true;
                      }
                      catch (ConsumeException ex)
                      {
                          IsReadonly = true;
                          consumer.Close();
                          await MessageBoxManager.GetMessageBoxStandard("错误", ex.Message, ButtonEnum.Ok).ShowWindowDialogAsync(App.MainWindow);
                      }
                      catch (Exception ex)
                      {
                          IsReadonly = true;
                          consumer.Close();
                          await MessageBoxManager.GetMessageBoxStandard("错误", ex.Message, ButtonEnum.Ok).ShowWindowDialogAsync(App.MainWindow);
                      }
                  }
              });

        }

        [RelayCommand]
        void CancelSubcribe()
        {
            cts.Cancel();
            cts = new CancellationTokenSource();
            IsReadonly = true;
            ConsumerMessage = "";
        }
    }
}
