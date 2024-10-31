using KafkaToolWpf.Models;
using Prism.Dialogs;
using Prism.Mvvm;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace KafkaToolWpf.ViewModels
{
    public class TopicListDialogViewModel : BindableBase, IDialogAware
    {

        private List<Topic> _topics;
        public List<Topic> TopicList
        {
            get { return _topics; }
            set { SetProperty(ref _topics, value); }
        }

        public DialogCloseListener RequestClose { get; set; } = new DialogCloseListener();

        public bool CanCloseDialog()
        {
            return true;
        }

        public void OnDialogClosed()
        {

        }

        public void OnDialogOpened(IDialogParameters parameters)
        {
            string data = parameters.GetValue<string>("TopicList");
            TopicList = JsonSerializer.Deserialize<List<Topic>>(data);
        }
    }
}
