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
            //containerRegistry.RegisterSingleton<IMessageService, MessageService>();
            containerRegistry.RegisterDialog<TopicListDialogWindow, TopicListDialogViewModel>();
        }

        protected override void ConfigureModuleCatalog(IModuleCatalog moduleCatalog)
        {
            //moduleCatalog.AddModule<ModuleNameModule>();
        }
    }
}
