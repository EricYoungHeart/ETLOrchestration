using System.Text.Json;
using Etl.Orchestrator;
using Etl.Orchestrator.Data;
using Etl.Orchestrator.Domain;
using Etl.Orchestrator.Services;
using Microsoft.OpenApi.Models;

var builder = WebApplication.CreateBuilder(args);

/// <summary>
/// Регистрация конфигурации приложения из секции "App" в appsettings.json
/// </summary>
builder.Services.Configure<AppSettings>(builder.Configuration.GetSection("App"));

/// <summary>
/// Репозиторий для работы с заданиями (доступ к таблице etl_job)
/// Регистрируется как Singleton (один экземпляр на всё приложение)
/// </summary>
builder.Services.AddSingleton<JobRepository>();

/// <summary>
/// Сервис для запуска внешних процессов (Python-скриптов)
/// Регистрируется как Singleton
/// </summary>
builder.Services.AddSingleton<ProcessRunner>();

/// <summary>
/// Фоновый воркер, который обрабатывает очередь заданий
/// </summary>
builder.Services.AddHostedService<JobWorker>();

/// <summary>
/// Подключение генерации спецификации OpenAPI/Swagger
/// </summary>
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(c => c.SwaggerDoc("v1", new OpenApiInfo { Title = "ETL Orchestrator", Version = "v1" }));

var app = builder.Build();

/// <summary>
/// Подключение Swagger UI (доступно по адресу /swagger)
/// </summary>
app.UseSwagger();
app.UseSwaggerUI();

/// <summary>
/// POST /runs
/// Запускает новый ETL-процесс (если нет активного для тех же period и q).
/// Возвращает 201 Created с идентификатором задания.
/// </summary>
app.MapPost("/runs", async (JobRepository repo, JobStartReq req) =>
{
    // Проверка идемпотентности: если уже есть активный run с такими period+q, возвращаем 409 Conflict
    if (await repo.ExistsActiveAsync("monthly_load", req.Period, req.Q))
        return Results.Conflict(new { message = "Active run for (period,q) already exists" });

    var payload = JsonSerializer.Serialize(new
    {
        period = req.Period,
        q = req.Q,
        dryRun = req.DryRun ?? false,
        steps = req.Steps ?? new[] { "extract", "load", "enrich" }
    });

    var id = await repo.EnqueueAsync("monthly_load", payload);
    return Results.Created($"/runs/{id}", new { runId = id });
})
.WithOpenApi();

/// <summary>
/// GET /runs/{id}
/// Получает информацию о задании по его идентификатору.
/// Если не найдено → 404 Not Found.
/// </summary>
app.MapGet("/runs/{id:guid}", async (JobRepository repo, Guid id) =>
{
    var job = await repo.FindAsync(id);
    return job is null ? Results.NotFound() : Results.Ok(job);
}).WithOpenApi();

/// <summary>
/// GET /runs/{id}/logs
/// Возвращает лог-файл для указанного задания (если он существует).
/// </summary>
app.MapGet("/runs/{id:guid}/logs", async (JobRepository repo, Guid id) =>
{
    var job = await repo.FindAsync(id);
    if (job is null || string.IsNullOrWhiteSpace(job.LogPath) || !File.Exists(job.LogPath))
        return Results.NotFound();

    var stream = File.Open(job.LogPath!, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
    return Results.File(stream, "text/plain");
}).WithOpenApi();

/// <summary>
/// POST /runs/{id}/cancel
/// Отменяет выполнение задания по идентификатору.
/// Возвращает 202 Accepted.
/// </summary>
app.MapPost("/runs/{id:guid}/cancel", async (JobRepository repo, Guid id) =>
{
    await repo.CancelAsync(id);
    return Results.Accepted($"/runs/{id}");
}).WithOpenApi();

/// <summary>
/// Точка входа: запускаем приложение
/// </summary>
app.Run();

/// <summary>
/// DTO-модель для запуска нового задания (входной контракт POST /runs)
/// </summary>
record JobStartReq(string Period, string Q, bool? DryRun, string[]? Steps);
