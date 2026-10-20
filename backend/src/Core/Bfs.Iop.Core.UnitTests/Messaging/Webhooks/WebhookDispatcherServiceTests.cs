using AwesomeAssertions;
using Bfs.Iop.AuditTrail.Abstractions.Models;
using Bfs.Iop.Common.Messaging;
using Bfs.Iop.Core.Messaging.Webhooks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using System.Net;

namespace Bfs.Iop.Core.UnitTests.Messaging.Webhooks;

[TestFixture(TestOf = typeof(WebhookDispatcherService))]
internal sealed class WebhookDispatcherServiceTests
{
    [Test]
    public async Task Given_queued_webhook_When_dispatched_Then_resource_is_posted()
    {
        // Arrange
        var message = new WebhookMessage(Guid.NewGuid(), "CH_BFS", AuditTrailResourceType.Dataset, Guid.NewGuid());
        var handler = new CapturingHandler();
        var httpClientFactory = Substitute.For<IHttpClientFactory>();
        httpClientFactory.CreateClient(WebhookDispatcherService.HttpClientName).Returns(_ => new HttpClient(handler));

        var queue = new ChannelMessageQueue<WebhookMessage>();
        await queue.EnqueueAsync(message);

        var service = new WebhookDispatcherService(
            queue,
            httpClientFactory,
            Options.Create(new WebhookOptions { Endpoints = { ["CH_BFS"] = new("https://hooks.example/i14y") } }),
            Substitute.For<ILogger<WebhookDispatcherService>>());

        // Act
        await service.StartAsync(CancellationToken.None);
        var (request, body) = await handler.Received.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await service.StopAsync(CancellationToken.None);

        // Assert
        request.RequestUri.Should().Be(new Uri("https://hooks.example/i14y"));
        body.Should().Be($$"""{"resourceType":"Dataset","resourceId":"{{message.ResourceId}}"}""");
    }

    private sealed class CapturingHandler : HttpMessageHandler
    {
        public TaskCompletionSource<(HttpRequestMessage Request, string Body)> Received { get; } = new();

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Received.TrySetResult((request, await request.Content!.ReadAsStringAsync(cancellationToken)));
            return new HttpResponseMessage(HttpStatusCode.OK);
        }
    }
}
