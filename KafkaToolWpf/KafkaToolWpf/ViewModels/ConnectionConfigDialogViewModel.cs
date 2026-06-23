using KafkaToolWpf.Models;
using Prism.Commands;
using Prism.Dialogs;
using Prism.Mvvm;
using System.Collections.ObjectModel;

namespace KafkaToolWpf.ViewModels
{
    public class ConnectionConfigDialogViewModel : BindableBase, IDialogAware
    {
        private ObservableCollection<ConnectionConfig> _connections = new();
        public ObservableCollection<ConnectionConfig> Connections
        {
            get => _connections;
            set => SetProperty(ref _connections, value);
        }

        private ConnectionConfig _selectedConnection;
        public ConnectionConfig SelectedConnection
        {
            get => _selectedConnection;
            set
            {
                SetProperty(ref _selectedConnection, value);
                if (value != null)
                {
                    EditName = value.Name;
                    EditBootstrapServers = value.BootstrapServers;
                    EditUseSasl = value.UseSasl;
                    EditSaslMechanism = value.SaslMechanism;
                    EditSaslUsername = value.SaslUsername;
                    EditSaslPassword = value.SaslPassword;
                    EditUseSsl = value.UseSsl;
                    EditSslSkipVerify = value.SslSkipVerify;
                }
            }
        }

        private string _editName;
        public string EditName
        {
            get => _editName;
            set => SetProperty(ref _editName, value);
        }

        private string _editBootstrapServers;
        public string EditBootstrapServers
        {
            get => _editBootstrapServers;
            set => SetProperty(ref _editBootstrapServers, value);
        }

        private bool _editUseSasl;
        public bool EditUseSasl
        {
            get => _editUseSasl;
            set => SetProperty(ref _editUseSasl, value);
        }

        private string _editSaslMechanism = "PLAIN";
        public string EditSaslMechanism
        {
            get => _editSaslMechanism;
            set => SetProperty(ref _editSaslMechanism, value);
        }

        private string _editSaslUsername;
        public string EditSaslUsername
        {
            get => _editSaslUsername;
            set => SetProperty(ref _editSaslUsername, value);
        }

        private string _editSaslPassword;
        public string EditSaslPassword
        {
            get => _editSaslPassword;
            set => SetProperty(ref _editSaslPassword, value);
        }

        private bool _editUseSsl;
        public bool EditUseSsl
        {
            get => _editUseSsl;
            set => SetProperty(ref _editUseSsl, value);
        }

        private bool _editSslSkipVerify;
        public bool EditSslSkipVerify
        {
            get => _editSslSkipVerify;
            set => SetProperty(ref _editSslSkipVerify, value);
        }

        public DelegateCommand AddConnectionCommand { get; }
        public DelegateCommand DeleteConnectionCommand { get; }
        public DelegateCommand SaveCommand { get; }
        public DelegateCommand SelectAndCloseCommand { get; }

        public ConnectionConfigDialogViewModel()
        {
            AddConnectionCommand = new DelegateCommand(AddConnection);
            DeleteConnectionCommand = new DelegateCommand(DeleteConnection);
            SaveCommand = new DelegateCommand(Save);
            SelectAndCloseCommand = new DelegateCommand(SelectAndClose);
        }

        private void AddConnection()
        {
            if (string.IsNullOrWhiteSpace(EditName) || string.IsNullOrWhiteSpace(EditBootstrapServers))
                return;

            var conn = new ConnectionConfig
            {
                Name = EditName,
                BootstrapServers = EditBootstrapServers,
                UseSasl = EditUseSasl,
                SaslMechanism = EditSaslMechanism,
                SaslUsername = EditSaslUsername,
                SaslPassword = EditSaslPassword,
                UseSsl = EditUseSsl,
                SslSkipVerify = EditSslSkipVerify
            };

            Connections.Add(conn);
            ClearEditFields();
        }

        private void DeleteConnection()
        {
            if (SelectedConnection != null)
            {
                Connections.Remove(SelectedConnection);
                SelectedConnection = null;
                ClearEditFields();
            }
        }

        private void Save()
        {
            RequestClose.Invoke(new DialogParameters
            {
                { "action", "save" },
                { "connections", Connections }
            });
        }

        private void SelectAndClose()
        {
            if (SelectedConnection == null) return;
            RequestClose.Invoke(new DialogParameters
            {
                { "action", "select" },
                { "selectedConnection", SelectedConnection.Name }
            });
        }

        private void ClearEditFields()
        {
            EditName = "";
            EditBootstrapServers = "";
            EditUseSasl = false;
            EditSaslMechanism = "PLAIN";
            EditSaslUsername = "";
            EditSaslPassword = "";
            EditUseSsl = false;
            EditSslSkipVerify = false;
        }

        public DialogCloseListener RequestClose { get; set; } = new DialogCloseListener();
        public bool CanCloseDialog() => true;
        public void OnDialogClosed() { }

        public void OnDialogOpened(IDialogParameters parameters)
        {
            var connections = parameters.GetValue<ObservableCollection<ConnectionConfig>>("connections");
            if (connections != null)
                Connections = connections;
        }
    }
}