using System.Data;
using Dapper;
using Etl.Orchestrator.Domain;
using Npgsql;

namespace Etl.Orchestrator.Data
{
    /// <summary>
    /// Репозиторий для работы с очередью ETL‑заданий в таблице <c>etl.etl_job</c>.
    /// Инкапсулирует операции постановки в очередь, выборки для выполнения,
    /// и смены статусов (успех/ошибка/отмена).
    /// </summary>
    /// <remarks>
    /// <para>
    /// Репозиторий использует <see cref="Dapper"/> для лёгкого маппинга строк БД на доменную модель <see cref="Job"/> и
    /// <see cref="Npgsql"/> для подключения к PostgreSQL. Строка подключения берётся из конфигурации
    /// по ключу <c>"ConnectionStrings:Postgres"</c>.
    /// </para>
    /// <para>
    /// Важные моменты по конкурентности:
    /// </para>
    /// <list type="bullet">
    /// <item>
    /// <description>
    /// <b>TryDequeueAsync</b> использует конструкцию <c>FOR UPDATE SKIP LOCKED</c> в CTE, что позволяет нескольким
    /// воркерам безопасно разбирать очередь без взаимных блокировок: занятая строка будет пропущена,
    /// а другая свободная — выбрана.
    /// </description>
    /// </item>
    /// <item>
    /// <description>
    /// Индекс и частично‑уникальное ограничение на стороне БД (см. <c>etl_job_unique_active</c>) гарантируют, что одновременно
    /// не будет двух активных заданий одного типа и с одинаковыми <c>(period,q)</c> в <c>params</c>.
    /// </description>
    /// </item>
    /// </list>
    /// <para>
    /// Формат параметров задания хранится в <c>params</c> (JSONB). В запросах используется оператор <c>-&gt;&gt;</c>
    /// для извлечения текстового значения (<c>params-&gt;&gt;'period'</c>, <c>params-&gt;&gt;'q'</c>).
    /// </para>
    /// </remarks>
    public sealed class JobRepository
    {
        private readonly string _cs;

        /// <summary>
        /// Создаёт репозиторий, считывая строку подключения <c>Postgres</c> из конфигурации.
        /// </summary>
        /// <param name="cfg">Глобальная конфигурация приложения (<see cref="IConfiguration"/>).</param>
        public JobRepository(IConfiguration cfg) => _cs = cfg.GetConnectionString("Postgres")!;

        /// <summary>
        /// Открывает новое подключение к PostgreSQL.
        /// </summary>
        /// <remarks>
        /// Подключение создаётся на каждый вызов и закрывается по выходу из <c>using</c>.
        /// Такой шаблон («одна операция — одно подключение») хорошо сочетается с пулом соединений Npgsql.
        /// </remarks>
        private IDbConnection Open() => new NpgsqlConnection(_cs);

        /// <summary>
        /// Ставит новое задание в очередь со статусом <c>queued</c>.
        /// </summary>
        /// <param name="type">Тип задания (например, <c>"monthly_load"</c>).</param>
        /// <param name="paramsJson">JSON‑строка с параметрами запуска (будет приведена к <c>jsonb</c>).</param>
        /// <returns>Идентификатор созданного задания.</returns>
        /// <remarks>
        /// <para>
        /// Колонка <c>params</c> имеет тип <c>jsonb</c>, поэтому входная строка приводится через <c>CAST(@paramsJson AS jsonb)</c>.
        /// </para>
        /// <para>
        /// Идемпотентность активных запусков по <c>(job_type, period, q)</c> обеспечивается ограничением БД.
        /// При необходимости проверять существование активного задания заранее используйте <see cref="ExistsActiveAsync"/>.
        /// </para>
        /// </remarks>
        public async Task<Guid> EnqueueAsync(string type, string paramsJson)
        {
            var id = Guid.NewGuid();
            const string sql = """
                INSERT INTO etl.etl_job (id, status, job_type, params)
                VALUES (@id, 'queued', @type, CAST(@paramsJson AS jsonb))
            """;
            using var db = Open();
            await db.ExecuteAsync(sql, new { id, type, paramsJson });
            return id;
        }

        /// <summary>
        /// Возвращает задание по идентификатору или <c>null</c>, если не найдено.
        /// </summary>
        /// <param name="id">Идентификатор задания.</param>
        public async Task<Job?> FindAsync(Guid id)
        {
            const string sql = "SELECT * FROM etl.etl_job WHERE id = @id";
            using var db = Open();
            return await db.QuerySingleOrDefaultAsync<Job>(sql, new { id });
        }

        /// <summary>
        /// Пытается выбрать одно задание из очереди и пометить его как <c>running</c>.
        /// </summary>
        /// <returns>
        /// Объект <see cref="Job"/> для выполнения или <c>null</c>, если в очереди нет доступных задач.
        /// </returns>
        /// <remarks>
        /// <para>
        /// Запрос использует шаблон:
        /// </para>
        /// <code>
        /// WITH cte AS (
        ///   SELECT id FROM etl.etl_job
        ///   WHERE status = 'queued'
        ///   ORDER BY created_at
        ///   FOR UPDATE SKIP LOCKED
        ///   LIMIT 1
        /// )
        /// UPDATE etl.etl_job j
        /// SET status='running', started_at=now(), attempt=attempt+1
        /// FROM cte
        /// WHERE j.id = cte.id
        /// RETURNING j.*;
        /// </code>
        /// <para>
        /// Ключевые моменты:
        /// </para>
        /// <list type="bullet">
        /// <item><description><b>FOR UPDATE</b> — блокирует выбранную строку на время транзакции.</description></item>
        /// <item><description><b>SKIP LOCKED</b> — пропускает занятые (заблокированные) строки, что позволяет нескольким воркерам
        /// безопасно разбирать очередь параллельно.</description></item>
        /// <item><description><b>RETURNING j.*</b> — возвращает обновлённую строку, чтобы не делать повторный <c>SELECT</c>.</description></item>
        /// </list>
        /// </remarks>
        /// <example>
        /// Пример цикла воркера:
        /// <code language="csharp">
        /// var job = await repo.TryDequeueAsync();
        /// if (job is null) { await Task.Delay(1000); return; }
        /// // Выполнить работу и зафиксировать результат:
        /// await repo.MarkSucceededAsync(job.Id, logPath);
        /// </code>
        /// </example>
        public async Task<Job?> TryDequeueAsync()
        {
            const string sql = """
                WITH cte AS (
                  SELECT id FROM etl.etl_job
                  WHERE status = 'queued'
                  ORDER BY created_at
                  FOR UPDATE SKIP LOCKED
                  LIMIT 1
                )
                UPDATE etl.etl_job j
                SET status='running', started_at=now(), attempt=attempt+1
                FROM cte
                WHERE j.id = cte.id
                RETURNING j.*;
            """;
            using var db = Open();
            return await db.QuerySingleOrDefaultAsync<Job>(sql);
        }

        /// <summary>
        /// Помечает задание как успешно завершённое (<c>succeeded</c>) и фиксирует путь к логу.
        /// </summary>
        /// <param name="id">Идентификатор задания.</param>
        /// <param name="logPath">Путь к файлу лога выполнения.</param>
        public async Task MarkSucceededAsync(Guid id, string logPath)
        {
            const string sql = """
                UPDATE etl.etl_job
                SET status='succeeded', finished_at=now(), log_path=@logPath
                WHERE id=@id
            """;
            using var db = Open();
            await db.ExecuteAsync(sql, new { id, logPath });
        }

        /// <summary>
        /// Помечает задание как завершённое с ошибкой (<c>failed</c>), записывает лог и текст ошибки.
        /// </summary>
        /// <param name="id">Идентификатор задания.</param>
        /// <param name="logPath">Путь к файлу лога (может быть <c>null</c>).</param>
        /// <param name="error">Текст ошибки.</param>
        public async Task MarkFailedAsync(Guid id, string? logPath, string error)
        {
            const string sql = """
                UPDATE etl.etl_job
                SET status='failed', finished_at=now(), log_path=@logPath, error=@error
                WHERE id=@id
            """;
            using var db = Open();
            await db.ExecuteAsync(sql, new { id, logPath, error });
        }

        /// <summary>
        /// Отменяет задание (<c>canceled</c>), если оно ещё не выполнено: было в очереди или выполняется.
        /// </summary>
        /// <param name="id">Идентификатор задания.</param>
        /// <remarks>
        /// Обновление ограничено статусами <c>('queued','running')</c>, чтобы не перезаписывать финальные состояния.
        /// </remarks>
        public async Task CancelAsync(Guid id)
        {
            const string sql = """
                UPDATE etl.etl_job
                SET status='canceled', finished_at=now()
                WHERE id=@id AND status IN ('queued','running')
            """;
            using var db = Open();
            await db.ExecuteAsync(sql, new { id });
        }

        /// <summary>
        /// Проверяет, существует ли активное (ещё не завершённое) задание данного типа и с теми же параметрами периода/кода.
        /// </summary>
        /// <param name="type">Тип задания (<c>job_type</c>).</param>
        /// <param name="period">Значение <c>period</c> из <c>params</c> (например, <c>"202507"</c>).</param>
        /// <param name="q">Значение <c>q</c> из <c>params</c> (например, <c>"I2"</c>).</param>
        /// <returns><c>true</c>, если в таблице есть задание со статусом <c>'queued'</c> или <c>'running'</c> и совпадающими параметрами.</returns>
        /// <remarks>
        /// <para>
        /// В запросе используется оператор <c>-&gt;&gt;</c> для извлечения текстового значения из <c>jsonb</c>:
        /// <c>params-&gt;&gt;'period'</c>, <c>params-&gt;&gt;'q'</c>.
        /// </para>
        /// <para>
        /// Это мягкая проверка перед постановкой в очередь (<see cref="EnqueueAsync"/>). Жёсткую гарантию даёт
        /// уникальный частичный индекс <c>etl_job_unique_active</c> на стороне БД.
        /// </para>
        /// </remarks>
        /// <example>
        /// Пример использования:
        /// <code language="csharp">
        /// if (await repo.ExistsActiveAsync("monthly_load", "202507", "I2"))
        ///     return Results.Conflict("Active run already exists");
        /// </code>
        /// </example>
        public async Task<bool> ExistsActiveAsync(string type, string period, string q)
        {
            const string sql = """
                SELECT COUNT(*) FROM etl.etl_job
                WHERE job_type=@type
                  AND params->>'period'=@period
                  AND params->>'q'=@q
                  AND status IN ('queued','running')
            """;
            using var db = Open();
            var n = await db.ExecuteScalarAsync<int>(sql, new { type, period, q });
            return n > 0;
        }
    }
}
