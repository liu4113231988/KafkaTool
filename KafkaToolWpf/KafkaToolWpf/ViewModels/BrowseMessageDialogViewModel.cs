using KafkaToolWpf.Models;
using Microsoft.Win32;
using Prism.Commands;
using Prism.Dialogs;
using Prism.Mvvm;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Windows;

namespace KafkaToolWpf.ViewModels
{
    public class BrowseMessageDialogViewModel : BindableBase, IDialogAware
    {
        private List<MessageRecord> _allMessages = new();
        private List<MessageRecord> _messages = new();
        public List<MessageRecord> Messages
        {
            get => _messages;
            set => SetProperty(ref _messages, value);
        }

        private MessageRecord _selectedMessage;
        public MessageRecord SelectedMessage
        {
            get => _selectedMessage;
            set => SetProperty(ref _selectedMessage, value);
        }

        private string _topicName;
        public string TopicName
        {
            get => _topicName;
            set => SetProperty(ref _topicName, value);
        }

        private string _filterText;
        public string FilterText
        {
            get => _filterText;
            set
            {
                SetProperty(ref _filterText, value);
                ApplyFilter();
            }
        }

        public DelegateCommand CloseCommand { get; }
        public DelegateCommand ExportCommand { get; }
        public DelegateCommand CopyToProducerCommand { get; }

        public BrowseMessageDialogViewModel()
        {
            CloseCommand = new DelegateCommand(Close);
            ExportCommand = new DelegateCommand(ExportMessages);
            CopyToProducerCommand = new DelegateCommand(CopyToProducer);
        }

        private void Close()
        {
            RequestClose.Invoke(new DialogParameters { { "action", "close" } });
        }

        private void ApplyFilter()
        {
            var filter = FilterText?.Trim();
            if (string.IsNullOrWhiteSpace(filter))
            {
                Messages = _allMessages.ToList();
                return;
            }

            Messages = _allMessages
                .Where(m =>
                    (m.Key?.Contains(filter, System.StringComparison.OrdinalIgnoreCase) ?? false) ||
                    (m.Value?.Contains(filter, System.StringComparison.OrdinalIgnoreCase) ?? false) ||
                    (m.FormattedHeaders?.Contains(filter, System.StringComparison.OrdinalIgnoreCase) ?? false) ||
                    m.Topic.Contains(filter, System.StringComparison.OrdinalIgnoreCase))
                .ToList();
        }

        private void ExportMessages()
        {
            var dialog = new SaveFileDialog
            {
                Filter = "JSON files (*.json)|*.json|CSV files (*.csv)|*.csv|All files (*.*)|*.*",
                FileName = $"{TopicName}-messages.json",
                Title = "导出消息"
            };

            if (dialog.ShowDialog() != true)
            {
                return;
            }

            if (Path.GetExtension(dialog.FileName).Equals(".csv", System.StringComparison.OrdinalIgnoreCase))
            {
                var rows = new List<string> { "Topic,Partition,Offset,Timestamp,Key,Value,Headers" };
                rows.AddRange(Messages.Select(m =>
                    string.Join(",",
                        Csv(m.Topic),
                        Csv(m.Partition.ToString()),
                        Csv(m.Offset.ToString()),
                        Csv(m.FormattedTimestamp),
                        Csv(m.Key),
                        Csv(m.Value),
                        Csv(m.FormattedHeaders))));
                File.WriteAllLines(dialog.FileName, rows);
            }
            else
            {
                File.WriteAllText(dialog.FileName, JsonSerializer.Serialize(Messages, new JsonSerializerOptions
                {
                    WriteIndented = true
                }));
            }

            MessageBox.Show("消息已导出。", "导出成功", MessageBoxButton.OK, MessageBoxImage.Information);
        }

        private void CopyToProducer()
        {
            if (SelectedMessage == null)
            {
                MessageBox.Show("请先选择一条消息。", "提示", MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            RequestClose.Invoke(new DialogParameters
            {
                { "action", "copyToProducer" },
                { "message", SelectedMessage }
            });
        }

        public DialogCloseListener RequestClose { get; set; } = new DialogCloseListener();
        public bool CanCloseDialog() => true;
        public void OnDialogClosed() { }

        public void OnDialogOpened(IDialogParameters parameters)
        {
            var messages = parameters.GetValue<List<MessageRecord>>("messages");
            if (messages != null)
            {
                _allMessages = messages;
                Messages = messages.ToList();
                SelectedMessage = Messages.FirstOrDefault();
            }

            var topic = parameters.GetValue<string>("topic");
            if (!string.IsNullOrEmpty(topic))
                TopicName = topic;
        }

        private static string Csv(string value)
        {
            var safe = value ?? string.Empty;
            if (safe.Contains('"'))
            {
                safe = safe.Replace("\"", "\"\"");
            }
            return $"\"{safe}\"";
        }
    }
}
