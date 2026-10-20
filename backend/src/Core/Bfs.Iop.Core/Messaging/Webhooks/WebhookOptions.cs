namespace Bfs.Iop.Core.Messaging.Webhooks;

public sealed class WebhookOptions
{
    public const string SectionName = "Webhooks";

    public Dictionary<string, WebhookEndpoint> Endpoints { get; init; } = [];
}

public sealed record WebhookEndpoint(Uri Url, string Secret);
