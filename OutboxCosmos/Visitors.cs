namespace OutboxCosmos;

public interface IMessageVisitor
{
    void Visit(RFQRequest message);
    void Visit(Quote message);
    void Visit(QuoteCancel message);
}

public interface IMessageVisitor<out T>
{
    T Visit(RFQRequest message);
    T Visit(Quote message);
    T Visit(QuoteCancel message);
}

public interface IRoutingVisitor : IMessageVisitor<IReadOnlyCollection<string>> { }

public interface IAsyncMessageVisitor : IMessageVisitor<Task<Result>> { }

public interface IAsyncMessageVisitor<TContext>
{
    Task<Result> Visit(TContext context, RFQRequest message);
    Task<Result> Visit(TContext context, Quote message);
    Task<Result> Visit(TContext context, QuoteCancel message);
}

public class LoggingVisitor : IMessageVisitor
{
    public void Visit(RFQRequest message) => Console.WriteLine($"[LOG] RFQRequest: {message.RFQId}, {message.Symbol}, {message.Quantity}");

    public void Visit(Quote message) => Console.WriteLine($"[LOG] Quote: {message.QuoteId}, RFQ={message.RFQId}, Price={message.Price}");

    public void Visit(QuoteCancel message) => Console.WriteLine($"[LOG] QuoteCancel: {message.QuoteId}, Reason={message.Reason}");
}