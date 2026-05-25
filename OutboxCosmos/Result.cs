namespace OutboxCosmos;

public abstract record Result
{
    public bool IsSuccess => this is Success;
    public bool IsFailure => this is Failure;

    public static Result Ok(string? text = null) => new Success(text);

    public static Result Fail(string message, bool isRetryable = false, Exception? ex = null)
        => new Failure(message, isRetryable, ex);
}

public sealed record Success(string? Text = null) : Result;

public sealed record Failure(string ErrorMessage, bool IsRetryable, Exception? Exception = null) : Result;