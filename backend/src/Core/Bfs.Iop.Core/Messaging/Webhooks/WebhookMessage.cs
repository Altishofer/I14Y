using Bfs.Iop.AuditTrail.Abstractions.Models;

namespace Bfs.Iop.Core.Messaging.Webhooks;

internal sealed record WebhookMessage(Guid Id, string AgentIdentifier, AuditTrailResourceType ResourceType, Guid ResourceId, int FailCount = 0)
{ }
