using System;

namespace KafkaToolWpf.Services
{
    public interface IAppLogger
    {
        void Info(string message);
        void Warn(string message);
        void Error(string message, Exception exception = null);
    }
}
