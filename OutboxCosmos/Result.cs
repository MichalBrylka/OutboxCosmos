namespace OutboxCosmos;

public abstract record Result
{
    public bool IsSuccess => this is Success;
    public bool IsFailure => this is Failure;

    public static Result Ok(string? text = null) => new Success(text);

    public static Result Fail(string message, bool isRetryable = false, Exception? ex = null, string? id = null)
        => new Failure(message, isRetryable, ex, id);
}

public sealed record Success(string? Text = null) : Result
{
    public override string ToString() => $"✅ Success {{ Text = {Text} }}";
}

public sealed record Failure(string ErrorMessage, bool IsRetryable, Exception? Exception = null, string? Id = null) : Result
{
    public override string ToString() =>
        $"❌ Failure {{ Error = {ErrorMessage}, IsRetryable = {(IsRetryable ? " 🔄" : "⛔")}, Exception = {Exception}, Id = {Id} }}";
}