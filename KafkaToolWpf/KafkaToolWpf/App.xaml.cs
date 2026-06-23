using KafkaToolWpf.Services;
using KafkaToolWpf.ViewModels;
using KafkaToolWpf.Views;
using Prism.Dialogs;
using Prism.Ioc;
using Prism.Modularity;
using System;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Threading;

namespace KafkaToolWpf
{
    /// <summary>
    /// Interaction logic for App.xaml
    /// </summary>
    public partial class App
    {
        public App()
        {
            DispatcherUnhandledException += OnDispatcherUnhandledException;
            AppDomain.CurrentDomain.UnhandledException += OnCurrentDomainUnhandledException;
            TaskScheduler.UnobservedTaskException += OnUnobservedTaskException;
        }

        protected override Window CreateShell()
        {
            return Container.Resolve<MainWindow>();
        }

        protected override void RegisterTypes(IContainerRegistry containerRegistry)
        {
            // Register services
            containerRegistry.RegisterSingleton<IKafkaService, KafkaService>();
            containerRegistry.RegisterSingleton<IAppLogger, FileAppLogger>();

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

        private void OnDispatcherUnhandledException(object sender, DispatcherUnhandledExceptionEventArgs e)
        {
            Container.Resolve<IAppLogger>().Error("UI 线程发生未处理异常。", e.Exception);
        }

        private void OnCurrentDomainUnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            Container.Resolve<IAppLogger>().Error(
                $"应用域发生未处理异常，IsTerminating={e.IsTerminating}。",
                e.ExceptionObject as Exception);
        }

        private void OnUnobservedTaskException(object sender, UnobservedTaskExceptionEventArgs e)
        {
            Container.Resolve<IAppLogger>().Error("后台任务发生未观察异常。", e.Exception);
            e.SetObserved();
        }
    }
}
