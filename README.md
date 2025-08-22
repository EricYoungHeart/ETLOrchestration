ETL Orchestrator (Minimal API + Background Worker)

Шаблон минималистичного REST API для запуска и мониторинга ETL‑заданий. Проект демонстрирует связку Minimal APIs + фоновый воркер с ограничением параллелизма, запуск внешних процессов (Python) и документирование через Swagger/OpenAPI.

Что внутри

✅ Minimal APIs + Swagger/OpenAPI — быстрый старт, авто‑документация.

✅ Фоновый воркер с SemaphoreSlim — контролируем количество параллельных задач.

✅ Безопасный dequeue из БД с FOR UPDATE SKIP LOCKED — отсутствие гонок при выборе задач.

✅ Разделение обязанностей: JobRepository (БД), ProcessRunner (процессы), JobWorker (оркестрация).

✅ Конфигурация через IOptions<AppSettings> — Python, таймауты, директория логов, пул воркера.

✅ Логи выполнения — stdout/stderr внешнего процесса в файл, доступ по API.

Архитектура (коротко)

✅ JobRepository — очередь заданий (enqueue/dequeue, mark success/fail/cancel, поиск по id).

✅ ProcessRunner — запуск внешнего процесса (напр., python <script> --period ...), таймаут, стриминг логов.

✅ JobWorker (BackgroundService) — бесконечный цикл: забрать задачу → запустить процесс → сохранить статус.

✅ Program — DI, Swagger, маршруты Minimal API.

Для чего подходит

🆗 Шаблон для сервисов, запускающих/контролирующих ETL/скрипты/CLI‑утилиты.

🆗 Базис для минимального REST API с фоновыми задачами и очередью в БД.

🆗 Быстрый старт PoC/пилотов с прозрачной архитектурой.

Планы (roadmap)

🗓️ ProblemDetails + валидация входа.

🗓️ Версионирование API и Rate Limiting.

🗓️ Аутентификация (API‑Key/JWT), CORS.

🗓️ Health Checks, Serilog, OpenTelemetry.

🗓️ Dockerfile/Compose и тесты (интеграционные + Testcontainers).
