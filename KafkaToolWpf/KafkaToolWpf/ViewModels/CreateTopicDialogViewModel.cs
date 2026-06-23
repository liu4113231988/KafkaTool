using Prism.Commands;
using Prism.Dialogs;
using Prism.Mvvm;

namespace KafkaToolWpf.ViewModels
{
    public class CreateTopicDialogViewModel : BindableBase, IDialogAware
    {
        private string _topicName;
        public string TopicName
        {
            get => _topicName;
            set => SetProperty(ref _topicName, value);
        }

        private int _partitionCount = 3;
        public int PartitionCount
        {
            get => _partitionCount;
            set => SetProperty(ref _partitionCount, value);
        }

        private int _replicationFactor = 1;
        public int ReplicationFactor
        {
            get => _replicationFactor;
            set => SetProperty(ref _replicationFactor, value);
        }

        private string _configEntries;
        public string ConfigEntries
        {
            get => _configEntries;
            set => SetProperty(ref _configEntries, value);
        }

        private string _errorMessage;
        public string ErrorMessage
        {
            get => _errorMessage;
            set => SetProperty(ref _errorMessage, value);
        }

        public DelegateCommand CreateCommand { get; }
        public DelegateCommand CancelCommand { get; }

        public CreateTopicDialogViewModel()
        {
            CreateCommand = new DelegateCommand(Create, CanCreate)
                .ObservesProperty(() => TopicName)
                .ObservesProperty(() => PartitionCount)
                .ObservesProperty(() => ReplicationFactor);
            CancelCommand = new DelegateCommand(Cancel);
        }

        private bool CanCreate()
        {
            return !string.IsNullOrWhiteSpace(TopicName) && PartitionCount > 0 && ReplicationFactor > 0;
        }

        private void Create()
        {
            var configDict = new System.Collections.Generic.Dictionary<string, string>();
            if (!string.IsNullOrWhiteSpace(ConfigEntries))
            {
                foreach (var line in ConfigEntries.Split('\n', System.StringSplitOptions.RemoveEmptyEntries))
                {
                    var parts = line.Split('=', 2);
                    if (parts.Length == 2)
                        configDict[parts[0].Trim()] = parts[1].Trim();
                }
            }

            var parameters = new DialogParameters
            {
                { "action", "create" },
                { "topicName", TopicName },
                { "partitions", PartitionCount },
                { "replicationFactor", (short)ReplicationFactor },
                { "configs", configDict }
            };

            RequestClose.Invoke(parameters);
        }

        private void Cancel()
        {
            RequestClose.Invoke(new DialogParameters { { "action", "cancel" } });
        }

        public DialogCloseListener RequestClose { get; set; } = new DialogCloseListener();

        public bool CanCloseDialog() => true;
        public void OnDialogClosed() { }
        public void OnDialogOpened(IDialogParameters parameters) { }
    }
}