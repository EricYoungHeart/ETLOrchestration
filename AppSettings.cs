namespace Etl.Orchestrator
{
    /// <summary>
    /// Корневой класс конфигурации приложения ETL Orchestrator.
    /// Загружается из настроек (например, из <c>appsettings.json</c>)
    /// и содержит параметры для Python-скриптов, логов и фонового воркера.
    /// </summary>
    /// <remarks>
    /// Используется через внедрение зависимостей (DI) в .NET:
    /// <code>
    /// builder.Services.Configure&lt;AppSettings&gt;(builder.Configuration.GetSection("App"));
    /// </code>
    /// Таким образом, объект <c>AppSettings</c> отражает секцию <c>"App"</c> в конфигурации.
    /// </remarks>
    public sealed class AppSettings
    {
        /// <summary>
        /// Настройки для запуска Python-скриптов.
        /// </summary>
        public required PythonSettings Python { get; init; }

        /// <summary>
        /// Настройки логирования ETL-процессов.
        /// </summary>
        public required LogsSettings Logs { get; init; }

        /// <summary>
        /// Настройки фонового воркера, который обрабатывает задания.
        /// </summary>
        public required WorkerSettings Worker { get; init; }

        /// <summary>
        /// Конфигурация запуска Python-процессов.
        /// </summary>
        /// <remarks>
        /// Определяет, какой исполняемый файл Python использовать,
        /// какой скрипт запускать и сколько секунд ждать завершения.
        /// </remarks>
        public sealed class PythonSettings
        {
            /// <summary>
            /// Путь к исполняемому файлу Python (например, "python3" или полный путь).
            /// </summary>
            public required string Executable { get; init; }

            /// <summary>
            /// Путь к Python-скрипту, который выполняется для ETL-процессов.
            /// </summary>
            public required string Script { get; init; }

            /// <summary>
            /// Максимальное время ожидания выполнения скрипта (в секундах).
            /// По умолчанию 7200 секунд (2 часа).
            /// </summary>
            public int TimeoutSeconds { get; init; } = 7200;
        }

        /// <summary>
        /// Конфигурация логирования.
        /// </summary>
        /// <remarks>
        /// Определяет директорию, куда сохраняются логи выполнения заданий.
        /// </remarks>
        public sealed class LogsSettings
        {
            /// <summary>
            /// Директория для логов ETL-процессов.
            /// </summary>
            public required string Dir { get; init; }
        }

        /// <summary>
        /// Конфигурация фонового воркера, который выполняет задания ETL.
        /// </summary>
        /// <remarks>
        /// Эти параметры управляют самим сервисом <c>JobWorker</c>:
        /// - Включён ли он;
        /// - Как часто опрашивать базу (polling);
        /// - Сколько заданий выполнять параллельно.
        /// </remarks>
        public sealed class WorkerSettings
        {
            /// <summary>
            /// Флаг включения/выключения фонового воркера.
            /// По умолчанию включён.
            /// </summary>
            public bool Enabled { get; init; } = true;

            /// <summary>
            /// Интервал опроса базы данных (в миллисекундах).
            /// По умолчанию 1000 мс (1 секунда).
            /// </summary>
            public int PollMillis { get; init; } = 1000;

            /// <summary>
            /// Максимальное количество одновременных выполняемых заданий.
            /// По умолчанию 1.
            /// </summary>
            public int MaxConcurrent { get; init; } = 1;
        }
    }
}
