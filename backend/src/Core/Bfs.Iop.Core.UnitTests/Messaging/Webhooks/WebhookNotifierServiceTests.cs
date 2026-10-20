using Bfs.Iop.AuditTrail.Abstractions.Models;
using Bfs.Iop.Common.Messaging;
using Bfs.Iop.Core.Messaging.AuditTrail;
using Bfs.Iop.Core.Messaging.Webhooks;
using Bfs.Iop.DataAccess.Abstractions;
using Bfs.Iop.DataAccess.Contracts;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Bfs.Iop.Core.UnitTests.Messaging.Webhooks;

[TestFixture(TestOf = typeof(WebhookNotifierService))]
internal sealed class WebhookNotifierServiceTests
{
    private IMessageQueue<WebhookMessage> _queue = null!;
    private IUnrestrictedReaderService _unrestrictedReaderService = null!;

    [SetUp]
    public void SetUp()
    {
        _queue = Substitute.For<IMessageQueue<WebhookMessage>>();
        _unrestrictedReaderService = Substitute.For<IUnrestrictedReaderService>();
    }

    [TestCase("CH_BAFU", 1)]
    [TestCase("CH_BFS", 0)]
    public async Task Given_dataset_structure_update_When_publisher_has_an_endpoint_Then_webhook_is_enqueued(string publisher, int expectedCount)
    {
        // Arrange
        var id = Guid.NewGuid();
        _unrestrictedReaderService.TryGetDatasetAsync(id, Arg.Any<CancellationToken>()).Returns(CreateDataset(id, publisher));

        var service = CreateService();

        // Act
        await service.NotifyResourceUpdatedAsync(AuditTrailResourceType.DatasetStructure, id, CancellationToken.None);

        // Assert
        await _queue.Received(expectedCount).EnqueueAsync(
            Arg.Is<WebhookMessage>(x => x.AgentIdentifier == publisher && x.ResourceType == AuditTrailResourceType.Dataset && x.ResourceId == id),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Given_dataset_deletion_When_resource_was_tracked_Then_webhook_is_enqueued_with_owner_resolved_before()
    {
        // Arrange
        var id = Guid.NewGuid();
        _unrestrictedReaderService.TryGetDatasetAsync(id, Arg.Any<CancellationToken>()).Returns(CreateDataset(id, "CH_BAFU"), (DcatDatasetModel?)null);

        var service = CreateService();

        // Act
        await service.EnsureResourceIsTrackedAsync(AuditTrailResourceType.Dataset, id, CancellationToken.None);
        await service.NotifyResourceDeletedAsync(AuditTrailResourceType.Dataset, id, CancellationToken.None);

        // Assert
        await _queue.Received(1).EnqueueAsync(
            Arg.Is<WebhookMessage>(x => x.AgentIdentifier == "CH_BAFU" && x.ResourceId == id),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Given_agent_update_When_webhooks_are_configured_Then_nothing_is_enqueued()
    {
        // Arrange
        var service = CreateService();

        // Act
        await service.NotifyResourceUpdatedAsync(AuditTrailResourceType.Agent, Guid.NewGuid(), CancellationToken.None);

        // Assert
        await _queue.DidNotReceiveWithAnyArgs().EnqueueAsync(default!, default);
    }

    private static DcatDatasetModel CreateDataset(Guid id, string publisher) =>
        new() { Id = id, Publisher = new() { Identifier = publisher, Name = new(), PrefLabel = new(), System = new() }, System = new() };

    private WebhookNotifierService CreateService() =>
        new(
            Substitute.For<IAuditTrailNotifierService>(),
            _queue,
            _unrestrictedReaderService,
            Options.Create(new WebhookOptions { Endpoints = { ["CH_BAFU"] = new(new("https://hooks.example/i14y"), "secret") } }),
            Substitute.For<ILogger<WebhookNotifierService>>());
}
