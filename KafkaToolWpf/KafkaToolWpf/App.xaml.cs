using KafkaToolWpf.Services;
using KafkaToolWpf.ViewModels;
using KafkaToolWpf.Views;
using Prism.Dialogs;
using Prism.Ioc;
using Prism.Modularity;
using System.Windows;

namespace KafkaToolWpf
{
    /// <summary>
    /// Interaction logic for App.xaml
    /// </summary>
    public partial class App
    {
        protected override Window CreateShell()
        {
            return Container.Resolve<MainWindow>();
        }

        protected override void RegisterTypes(IContainerRegistry containerRegistry)
        {
            // Register services
            containerRegistry.RegisterSingleton<IKafkaService, KafkaService>();

            // Register dialogs
            containerRegistry.RegisterDialog<TopicListDialogWindow, TopicListDialogViewModel>();
            containerRegistry.RegisterDialog<CreateTopicDialogWindow, CreateTopicDialogViewModel>();
            containerRegistry.RegisterDialog<BrowseMessageDialogWindow, BrowseMessageDialogViewModel>();
            containerRegistry.RegisterDialog<ConnectionConfigDialogWindow, ConnectionConfigDialogViewModel>();
        }

        protected override void ConfigureModuleCatalog(IModuleCatalog moduleCatalog)
        {
            //moduleCatalog.AddModule<ModuleNameModule>();
        }
    }
}