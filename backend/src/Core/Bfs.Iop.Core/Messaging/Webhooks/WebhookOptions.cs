namespace Bfs.Iop.Core.Messaging.Webhooks;

public sealed class WebhookOptions
{
    public const string SectionName = "Webhooks";

    public Dictionary<string, Uri> Endpoints { get; init; } = [];
}
