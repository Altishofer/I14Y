using Bfs.Iop.AuditTrail.Abstractions.Models;
using Bfs.Iop.Common.Messaging;
using Bfs.Iop.Core.Messaging.AuditTrail;
using Bfs.Iop.DataAccess.Abstractions;
using Bfs.Iop.DataAccess.Contracts;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Bfs.Iop.Core.Messaging.Webhooks;

internal sealed class WebhookNotifierService : IAuditTrailNotifierService
{
    private readonly IAuditTrailNotifierService _auditTrailNotifierService;
    private readonly IMessageQueue<WebhookMessage> _queue;
    private readonly IUnrestrictedReaderService _unrestrictedReaderService;
    private readonly WebhookOptions _options;
    private readonly ILogger<WebhookNotifierService> _logger;
    private readonly Dictionary<Guid, string> _ownersOfTrackedResources = [];

    public WebhookNotifierService(
        IAuditTrailNotifierService auditTrailNotifierService,
        IMessageQueue<WebhookMessage> queue,
        IUnrestrictedReaderService unrestrictedReaderService,
        IOptions<WebhookOptions> options,
        ILogger<WebhookNotifierService> logger)
    {
        _auditTrailNotifierService = auditTrailNotifierService;
        _queue = queue;
        _unrestrictedReaderService = unrestrictedReaderService;
        _options = options.Value;
        _logger = logger;
    }

    public async Task EnsureResourceIsTrackedAsync(
        AuditTrailResourceType resourceType,
        Guid id,
        CancellationToken cancellationToken = default)
    {
        await _auditTrailNotifierService.EnsureResourceIsTrackedAsync(resourceType, id, cancellationToken);

        // Remembered for a following deletion, after which the owner can no longer be read.
        if (await TryGetOwnerAsync(resourceType, id, cancellationToken) is { } owner)
        {
            _ownersOfTrackedResources[id] = owner;
        }
    }

    public Task NotifyResourceCreatedAsync(
        AuditTrailResourceType resourceType,
        Guid id,
        CancellationToken cancellationToken = default) =>
        EnqueueAsync(_auditTrailNotifierService.NotifyResourceCreatedAsync(resourceType, id, cancellationToken), resourceType, id, cancellationToken);

    public Task NotifyResourceUpdatedAsync(
        AuditTrailResourceType resourceType,
        Guid id,
        CancellationToken cancellationToken = default) =>
        EnqueueAsync(_auditTrailNotifierService.NotifyResourceUpdatedAsync(resourceType, id, cancellationToken), resourceType, id, cancellationToken);

    public Task NotifyResourceUpdatedAsync(
        AuditTrailResourceType resourceType,
        Guid id,
        Author author,
        CancellationToken cancellationToken = default) =>
        EnqueueAsync(_auditTrailNotifierService.NotifyResourceUpdatedAsync(resourceType, id, author, cancellationToken), resourceType, id, cancellationToken);

    public Task NotifyResourceDeletedAsync(
        AuditTrailResourceType resourceType,
        Guid id,
        CancellationToken cancellationToken = default) =>
        EnqueueAsync(_auditTrailNotifierService.NotifyResourceDeletedAsync(resourceType, id, cancellationToken), resourceType, id, cancellationToken);

    public Task NotifyResourceDeletedAsync(
        AuditTrailResourceType resourceType,
        Guid id,
        Author author,
        CancellationToken cancellationToken = default) =>
        EnqueueAsync(_auditTrailNotifierService.NotifyResourceDeletedAsync(resourceType, id, author, cancellationToken), resourceType, id, cancellationToken);

    public Task<Author> GetAuthorAsync(CancellationToken cancellationToken = default) =>
        _auditTrailNotifierService.GetAuthorAsync(cancellationToken);

    private async Task EnqueueAsync(Task auditTrailNotification, AuditTrailResourceType resourceType, Guid id, CancellationToken cancellationToken)
    {
        await auditTrailNotification;

        var owner = _ownersOfTrackedResources.GetValueOrDefault(id) ?? await TryGetOwnerAsync(resourceType, id, cancellationToken);

        if (owner is null || GetWebhookResourceType(resourceType) is not { } webhookResourceType)
        {
            return;
        }

        var message = new WebhookMessage(Guid.NewGuid(), owner, webhookResourceType, id);

        await _queue.EnqueueAsync(message, cancellationToken);
    }

    private async Task<string?> TryGetOwnerAsync(AuditTrailResourceType resourceType, Guid id, CancellationToken cancellationToken)
    {
        if (_options.Endpoints.Count == 0 || GetWebhookResourceType(resourceType) is not { } webhookResourceType)
        {
            return null;
        }

        try
        {
            IPublishableEntityModel? resource = webhookResourceType switch
            {
                AuditTrailResourceType.Dataset => await _unrestrictedReaderService.TryGetDatasetAsync(id, cancellationToken),
                AuditTrailResourceType.Concept => await _unrestrictedReaderService.TryGetIopConceptAsync(id, cancellationToken),
                AuditTrailResourceType.MappingTable => await _unrestrictedReaderService.TryGetMappingTableAsync(id, cancellationToken),
                AuditTrailResourceType.DataService => await _unrestrictedReaderService.TryGetDataServiceAsync(id, cancellationToken),
                _ => await _unrestrictedReaderService.TryGetPublicServiceAsync(id, cancellationToken),
            };

            var owner = resource?.Publisher.Identifier;

            return owner is not null && _options.Endpoints.ContainsKey(owner) ? owner : null;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Owner of {ResourceType} '{Id}' could not be resolved for webhooks.", resourceType, id);
            return null;
        }
    }

    private static AuditTrailResourceType? GetWebhookResourceType(AuditTrailResourceType resourceType) => resourceType switch
    {
        AuditTrailResourceType.Dataset or AuditTrailResourceType.DatasetStructure => AuditTrailResourceType.Dataset,
        AuditTrailResourceType.Concept or AuditTrailResourceType.ConceptCodeListEntries => AuditTrailResourceType.Concept,
        AuditTrailResourceType.MappingTable or AuditTrailResourceType.MappingTableRelations => AuditTrailResourceType.MappingTable,
        AuditTrailResourceType.DataService or AuditTrailResourceType.PublicService => resourceType,
        _ => null,
    };
}
