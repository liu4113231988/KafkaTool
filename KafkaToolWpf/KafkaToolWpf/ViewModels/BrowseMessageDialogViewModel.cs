using KafkaToolWpf.Models;
using Prism.Commands;
using Prism.Dialogs;
using Prism.Mvvm;
using System.Collections.Generic;

namespace KafkaToolWpf.ViewModels
{
    public class BrowseMessageDialogViewModel : BindableBase, IDialogAware
    {
        private List<MessageRecord> _messages = new();
        public List<MessageRecord> Messages
        {
            get => _messages;
            set => SetProperty(ref _messages, value);
        }

        private string _topicName;
        public string TopicName
        {
            get => _topicName;
            set => SetProperty(ref _topicName, value);
        }

        public DelegateCommand CloseCommand { get; }

        public BrowseMessageDialogViewModel()
        {
            CloseCommand = new DelegateCommand(Close);
        }

        private void Close()
        {
            RequestClose.Invoke(new DialogParameters { { "action", "close" } });
        }

        public DialogCloseListener RequestClose { get; set; } = new DialogCloseListener();
        public bool CanCloseDialog() => true;
        public void OnDialogClosed() { }

        public void OnDialogOpened(IDialogParameters parameters)
        {
            var messages = parameters.GetValue<List<MessageRecord>>("messages");
            if (messages != null)
                Messages = messages;

            var topic = parameters.GetValue<string>("topic");
            if (!string.IsNullOrEmpty(topic))
                TopicName = topic;
        }
    }
}