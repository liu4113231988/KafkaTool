using System;
using System.IO;
using System.Text;

namespace KafkaToolWpf.Services
{
    public sealed class FileAppLogger : IAppLogger
    {
        private static readonly object SyncRoot = new();
        private readonly string _logDirectory;

        public FileAppLogger()
        {
            _logDirectory = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                "KafkaTool",
                "logs");
        }

        public void Info(string message)
        {
            Write("INFO", message);
        }

        public void Warn(string message)
        {
            Write("WARN", message);
        }

        public void Error(string message, Exception exception = null)
        {
            var builder = new StringBuilder(message);
            if (exception != null)
            {
                builder.AppendLine();
                builder.AppendLine(exception.ToString());
            }

            Write("ERROR", builder.ToString());
        }

        private void Write(string level, string message)
        {
            var now = DateTime.Now;
            var filePath = Path.Combine(_logDirectory, $"kafkatool-{now:yyyyMMdd}.log");
            var line = $"[{now:yyyy-MM-dd HH:mm:ss.fff}] [{level}] {message}{Environment.NewLine}";

            lock (SyncRoot)
            {
                Directory.CreateDirectory(_logDirectory);
                File.AppendAllText(filePath, line, Encoding.UTF8);
            }
        }
    }
}
