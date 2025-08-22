using System.Diagnostics;

namespace Etl.Orchestrator.Services;

/// <summary>
/// Сервис-обёртка для запуска внешних процессов (например, Python-скриптов).
/// Позволяет задать исполняемый файл, скрипт, аргументы, таймаут и лог-файл.
/// Весь вывод процесса (stdout и stderr) записывается в лог.
/// </summary>
public sealed class ProcessRunner
{
    /// <summary>
    /// Запускает внешний процесс и ожидает его завершения.
    /// </summary>
    /// <param name="executable">Путь к исполняемому файлу (например, python.exe)</param>
    /// <param name="script">Сценарий/скрипт, который нужно выполнить</param>
    /// <param name="args">Аргументы командной строки</param>
    /// <param name="logFilePath">Путь к файлу для записи stdout и stderr</param>
    /// <param name="timeout">Максимальное время выполнения</param>
    /// <param name="ct">Токен отмены (например, при завершении приложения)</param>
    /// <returns>Код возврата процесса (0 = успех)</returns>
    public async Task<int> RunAsync(
        string executable, string script, IEnumerable<string> args,
        string logFilePath, TimeSpan timeout, CancellationToken ct)
    {
        // Убедимся, что каталог для логов существует
        Directory.CreateDirectory(Path.GetDirectoryName(logFilePath)!);

        // Настройка информации о процессе
        var psi = new ProcessStartInfo
        {
            FileName = executable,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false
        };
        psi.ArgumentList.Add(script);
        foreach (var a in args) psi.ArgumentList.Add(a);

        using var proc = new Process { StartInfo = psi, EnableRaisingEvents = true };
        using var log = new StreamWriter(File.Open(logFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

        // Подписка на события вывода/ошибок и запись в лог
        proc.OutputDataReceived += (_, e) => { if (e.Data != null) lock (log) { log.WriteLine(e.Data); log.Flush(); } };
        proc.ErrorDataReceived += (_, e) => { if (e.Data != null) lock (log) { log.WriteLine(e.Data); log.Flush(); } };

        // Запуск процесса
        proc.Start();
        proc.BeginOutputReadLine();
        proc.BeginErrorReadLine();

        // Таймаут + отмена по токену
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(timeout);

        try
        {
            // Асинхронное ожидание завершения
            await proc.WaitForExitAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            // Если таймаут или отмена → пробуем убить процесс
            try { if (!proc.HasExited) proc.Kill(true); } catch { /* ignore */ }
            await log.WriteLineAsync($"Process timed out or canceled after {timeout.TotalSeconds}s");
            return 124; // традиционный код возврата для "таймаут"
        }

        // Возврат кода завершения процесса
        return proc.ExitCode;
    }
}
