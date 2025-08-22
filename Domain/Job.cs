namespace Etl.Orchestrator.Domain
{
    /// <summary>
    /// Представляет задание ETL-процесса.
    /// Соответствует строке таблицы <c>etl.etl_job</c> в базе данных PostgreSQL.
    /// Хранит метаданные о запуске, статусе и результате выполнения.
    /// </summary>
    /// <remarks>
    /// Этот класс является доменной моделью (record) и используется для работы
    /// с заданиями ETL на уровне бизнес-логики.
    /// 
    /// Каждое свойство напрямую соответствует столбцу таблицы <c>etl.etl_job</c>.
    /// Таким образом, объект <c>Job</c> можно рассматривать как «проекцию» строки таблицы в код.
    /// </remarks>
    public record Job(
        /// <summary>
        /// Уникальный идентификатор задания (PRIMARY KEY в БД).
        /// </summary>
        Guid Id,

        /// <summary>
        /// Текущий статус выполнения.
        /// Возможные значения: queued, running, succeeded, failed, canceled.
        /// </summary>
        string Status,

        /// <summary>
        /// Тип задания (например, "monthly_load").
        /// Используется для классификации и контроля уникальности активных запусков.
        /// </summary>
        string JobType,

        /// <summary>
        /// Параметры запуска в формате JSON.
        /// Например: {"period":"202507","q":"I2"}.
        /// </summary>
        string ParamsJson,

        /// <summary>
        /// Время создания задания (момент постановки в очередь).
        /// </summary>
        DateTimeOffset CreatedAt,

        /// <summary>
        /// Время фактического начала выполнения (null, если ещё не запускалось).
        /// </summary>
        DateTimeOffset? StartedAt,

        /// <summary>
        /// Время завершения выполнения (успешного, отменённого или с ошибкой).
        /// </summary>
        DateTimeOffset? FinishedAt,

        /// <summary>
        /// Текущая попытка выполнения (для повторных запусков при ошибках).
        /// </summary>
        int Attempt,

        /// <summary>
        /// Максимальное количество допустимых попыток выполнения.
        /// </summary>
        int MaxAttempts,

        /// <summary>
        /// Путь к файлу журнала выполнения (лог).
        /// </summary>
        string? LogPath,

        /// <summary>
        /// Сообщение об ошибке, если задание завершилось неудачно.
        /// </summary>
        string? Error
    );
}
