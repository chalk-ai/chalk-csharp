namespace Chalk.Exceptions;

/// <summary>
/// Base class for all Chalk exceptions.
/// </summary>
public abstract class ChalkException : Exception
{
    protected ChalkException(string message) : base(message) { }
    protected ChalkException(string message, Exception innerException) : base(message, innerException) { }
}

/// <summary>
/// Exception for client-side errors (configuration, serialization, etc).
/// </summary>
public class ClientException : ChalkException
{
    public ClientException(string message) : base(message) { }
    public ClientException(string message, Exception innerException) : base(message, innerException) { }
}

/// <summary>
/// Exception for server-side errors (HTTP errors, API errors).
/// </summary>
public class ServerException : ChalkException
{
    public int StatusCode { get; }
    public string? ErrorCode { get; }
    public string? ErrorMessage { get; }

    public ServerException(int statusCode, string message) : base(message)
    {
        StatusCode = statusCode;
    }

    public ServerException(int statusCode, string? errorCode, string? errorMessage)
        : base($"Server error {statusCode}: {errorCode} - {errorMessage}")
    {
        StatusCode = statusCode;
        ErrorCode = errorCode;
        ErrorMessage = errorMessage;
    }
}
