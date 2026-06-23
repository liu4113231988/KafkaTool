using KafkaToolWpf.Models;
using Microsoft.Win32;
using Prism.Commands;
using Prism.Dialogs;
using Prism.Mvvm;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Windows;

namespace KafkaToolWpf.ViewModels
{
    public class ConnectionConfigDialogViewModel : BindableBase, IDialogAware
    {
        private string _initialSelectedConnectionName;

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
                    EditSslCaLocation = value.SslCaLocation;
                    EditSslCertificateLocation = value.SslCertificateLocation;
                    EditSslKeyLocation = value.SslKeyLocation;
                    EditSslSkipVerify = value.SslSkipVerify;
                }

                RaiseActionText();
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

        private string _editSslCaLocation;
        public string EditSslCaLocation
        {
            get => _editSslCaLocation;
            set => SetProperty(ref _editSslCaLocation, value);
        }

        private string _editSslCertificateLocation;
        public string EditSslCertificateLocation
        {
            get => _editSslCertificateLocation;
            set => SetProperty(ref _editSslCertificateLocation, value);
        }

        private string _editSslKeyLocation;
        public string EditSslKeyLocation
        {
            get => _editSslKeyLocation;
            set => SetProperty(ref _editSslKeyLocation, value);
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
        public DelegateCommand ImportCommand { get; }
        public DelegateCommand ExportCommand { get; }

        public IReadOnlyList<string> SaslMechanismOptions { get; } = new[]
        {
            "PLAIN",
            "SCRAM-SHA-256",
            "SCRAM-SHA-512",
            "GSSAPI"
        };

        public string UpsertButtonText => SelectedConnection == null ? "添加连接" : "保存编辑";

        public ConnectionConfigDialogViewModel()
        {
            AddConnectionCommand = new DelegateCommand(AddConnection);
            DeleteConnectionCommand = new DelegateCommand(DeleteConnection);
            SaveCommand = new DelegateCommand(Save);
            SelectAndCloseCommand = new DelegateCommand(SelectAndClose);
            ImportCommand = new DelegateCommand(ImportConnections);
            ExportCommand = new DelegateCommand(ExportConnections);
        }

        private void AddConnection()
        {
            if (string.IsNullOrWhiteSpace(EditName) || string.IsNullOrWhiteSpace(EditBootstrapServers))
                return;

            var conn = BuildConnectionFromEditor();
            var existing = SelectedConnection ?? Connections.FirstOrDefault(c =>
                c.Name == conn.Name && !ReferenceEquals(c, SelectedConnection));

            if (existing == null)
            {
                Connections.Add(conn);
                SelectedConnection = conn;
                return;
            }

            existing.Name = conn.Name;
            existing.BootstrapServers = conn.BootstrapServers;
            existing.UseSasl = conn.UseSasl;
            existing.SaslMechanism = conn.SaslMechanism;
            existing.SaslUsername = conn.SaslUsername;
            existing.SaslPassword = conn.SaslPassword;
            existing.UseSsl = conn.UseSsl;
            existing.SslSkipVerify = conn.SslSkipVerify;

            RaisePropertyChanged(nameof(Connections));
            RaiseActionText();
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
                { "connections", CloneConnections(Connections) }
            });
        }

        private void SelectAndClose()
        {
            if (SelectedConnection == null) return;
            RequestClose.Invoke(new DialogParameters
            {
                { "action", "select" },
                { "connections", CloneConnections(Connections) },
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
            EditSslCaLocation = "";
            EditSslCertificateLocation = "";
            EditSslKeyLocation = "";
            EditSslSkipVerify = false;
            RaiseActionText();
        }

        private ConnectionConfig BuildConnectionFromEditor()
        {
            return new ConnectionConfig
            {
                Name = EditName?.Trim(),
                BootstrapServers = EditBootstrapServers?.Trim(),
                UseSasl = EditUseSasl,
                SaslMechanism = EditSaslMechanism,
                SaslUsername = EditSaslUsername?.Trim(),
                SaslPassword = EditSaslPassword,
                UseSsl = EditUseSsl,
                SslCaLocation = EditSslCaLocation?.Trim(),
                SslCertificateLocation = EditSslCertificateLocation?.Trim(),
                SslKeyLocation = EditSslKeyLocation?.Trim(),
                SslSkipVerify = EditSslSkipVerify
            };
        }

        private static ObservableCollection<ConnectionConfig> CloneConnections(IEnumerable<ConnectionConfig> source)
        {
            return new ObservableCollection<ConnectionConfig>(source.Select(CloneConnection));
        }

        private static ConnectionConfig CloneConnection(ConnectionConfig source)
        {
            return new ConnectionConfig
            {
                Name = source.Name,
                BootstrapServers = source.BootstrapServers,
                UseSasl = source.UseSasl,
                SaslMechanism = source.SaslMechanism,
                SaslUsername = source.SaslUsername,
                SaslPassword = source.SaslPassword,
                UseSsl = source.UseSsl,
                SslCaLocation = source.SslCaLocation,
                SslCertificateLocation = source.SslCertificateLocation,
                SslKeyLocation = source.SslKeyLocation,
                SslSkipVerify = source.SslSkipVerify
            };
        }

        private void RaiseActionText()
        {
            RaisePropertyChanged(nameof(UpsertButtonText));
        }

        private void ImportConnections()
        {
            var dialog = new OpenFileDialog
            {
                Filter = "JSON files (*.json)|*.json|All files (*.*)|*.*",
                Title = "导入连接配置"
            };

            if (dialog.ShowDialog() != true)
            {
                return;
            }

            var json = File.ReadAllText(dialog.FileName);
            var imported = JsonSerializer.Deserialize<List<ConnectionConfig>>(json) ?? new List<ConnectionConfig>();

            Connections = new ObservableCollection<ConnectionConfig>(imported.Select(CloneConnection));
            SelectedConnection = Connections.FirstOrDefault();
        }

        private void ExportConnections()
        {
            var dialog = new SaveFileDialog
            {
                Filter = "JSON files (*.json)|*.json|All files (*.*)|*.*",
                FileName = "kafka-connections.json",
                Title = "导出连接配置"
            };

            if (dialog.ShowDialog() != true)
            {
                return;
            }

            var json = JsonSerializer.Serialize(Connections, new JsonSerializerOptions
            {
                WriteIndented = true
            });

            File.WriteAllText(dialog.FileName, json);
            MessageBox.Show("连接配置已导出。", "导出成功", MessageBoxButton.OK, MessageBoxImage.Information);
        }

        public DialogCloseListener RequestClose { get; set; } = new DialogCloseListener();
        public bool CanCloseDialog() => true;
        public void OnDialogClosed() { }

        public void OnDialogOpened(IDialogParameters parameters)
        {
            var connections = parameters.GetValue<ObservableCollection<ConnectionConfig>>("connections");
            if (connections != null)
                Connections = CloneConnections(connections);

            _initialSelectedConnectionName = parameters.GetValue<string>("selectedConnection");
            if (!string.IsNullOrWhiteSpace(_initialSelectedConnectionName))
            {
                SelectedConnection = Connections.FirstOrDefault(c => c.Name == _initialSelectedConnectionName);
            }
        }
    }
}
