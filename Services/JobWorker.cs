using System.Text.Json;
using Etl.Orchestrator;
using Etl.Orchestrator.Data;
using Microsoft.Extensions.Options;

namespace Etl.Orchestrator.Services;

/// <summary>
/// Фоновый рабочий сервис, который выбирает задания из БД и выполняет их
/// при помощи внешнего процесса (Python-скрипта).
/// Наследуется от <see cref="BackgroundService"/>, поэтому автоматически
/// запускается хостом при старте приложения.
/// </summary>
public sealed class JobWorker : BackgroundService
{
    private readonly JobRepository _repo;
    private readonly ProcessRunner _runner;
    private readonly AppSettings _cfg;
    private readonly SemaphoreSlim _slots;

    /// <summary>
    /// Конструктор получает зависимости через DI:
    /// - <see cref="JobRepository"/> для доступа к таблице заданий,
    /// - <see cref="ProcessRunner"/> для запуска внешних процессов,
    /// - <see cref="AppSettings"/> для конфигурации.
    /// Инициализируется семафор для ограничения числа параллельных заданий.
    /// </summary>
    public JobWorker(JobRepository repo, ProcessRunner runner, IOptions<AppSettings> cfg)
    {
        _repo = repo;
        _runner = runner;
        _cfg = cfg.Value;
        _slots = new SemaphoreSlim(_cfg.Worker.MaxConcurrent);
    }

    /// <summary>
    /// Основной цикл фонового сервиса.
    /// Пока приложение не остановлено, периодически опрашивает таблицу заданий
    /// и пытается забрать одно из них на выполнение.
    /// Контролирует число параллельных запусков с помощью <see cref="SemaphoreSlim"/>.
    /// </summary>
    /// <param name="stoppingToken">Токен отмены, сигнализирует об остановке приложения</param>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_cfg.Worker.Enabled) return;

        var pollDelay = TimeSpan.FromMilliseconds(_cfg.Worker.PollMillis);

        while (!stoppingToken.IsCancellationRequested)
        {
            await _slots.WaitAsync(stoppingToken);
            try
            {
                var job = await _repo.TryDequeueAsync();
                if (job is null)
                {
                    _slots.Release();
                    await Task.Delay(pollDelay, stoppingToken);
                    continue;
                }

                // Запуск отдельной задачи на выполнение job
                _ = Task.Run(() => RunJobAsync(job.Id, job.ParamsJson, stoppingToken)
                    .ContinueWith(_ => _slots.Release()), stoppingToken);
            }
            catch
            {
                _slots.Release();
                await Task.Delay(pollDelay, stoppingToken);
            }
        }
    }

    /// <summary>
    /// Выполняет одно конкретное задание:
    /// - парсит JSON с параметрами,
    /// - формирует аргументы командной строки,
    /// - запускает Python-скрипт с этими параметрами,
    /// - логирует результат и обновляет статус задания в БД.
    /// </summary>
    /// <param name="jobId">Идентификатор задания</param>
    /// <param name="paramsJson">JSON с параметрами запуска</param>
    /// <param name="ct">Токен отмены</param>
    private async Task RunJobAsync(Guid jobId, string paramsJson, CancellationToken ct)
    {
        string period = "", q = "";
        try
        {
            var p = JsonSerializer.Deserialize<Dictionary<string, object>>(paramsJson)!;
            period = p.TryGetValue("period", out var v1) ? v1?.ToString() ?? "" : "";
            q = p.TryGetValue("q", out var v2) ? v2?.ToString() ?? "" : "";
            var dry = p.TryGetValue("dryRun", out var v3) && v3 is bool b && b;

            var steps = p.TryGetValue("steps", out var v4) && v4 is IEnumerable<object> arr
                      ? arr.Select(o => o.ToString()).Where(s => !string.IsNullOrWhiteSpace(s)).ToArray()
                      : new[] { "extract", "load", "enrich" };

            var args = new List<string> { "--period", period, "--q", q };
            if (dry) args.Add("--dry-run");
            if (steps.Length > 0) { args.Add("--steps"); args.Add(string.Join(",", steps)); }

            var logPath = Path.Combine(_cfg.Logs.Dir, $"{period}_{q}_{jobId}.log");

            // Запуск Python-скрипта и ожидание завершения
            var exit = await _runner.RunAsync(
                _cfg.Python.Executable,
                _cfg.Python.Script,
                args,
                logPath,
                TimeSpan.FromSeconds(_cfg.Python.TimeoutSeconds),
                ct);

            if (exit == 0)
            {
                // Успешное завершение → помечаем задание выполненным
                await _repo.MarkSucceededAsync(jobId, logPath);
            }
            else
            {
                // Ошибка завершения → фиксируем неудачу
                await _repo.MarkFailedAsync(jobId, logPath, $"Exit code {exit}");
            }
        }
        catch (Exception ex)
        {
            // Любая непойманная ошибка → помечаем задание проваленным
            await _repo.MarkFailedAsync(jobId, null, ex.ToString());
        }
    }
}
