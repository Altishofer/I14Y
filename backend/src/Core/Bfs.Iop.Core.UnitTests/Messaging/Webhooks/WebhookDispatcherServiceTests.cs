using AwesomeAssertions;
using Bfs.Iop.AuditTrail.Abstractions.Models;
using Bfs.Iop.Common.Messaging;
using Bfs.Iop.Core.Messaging.Webhooks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using System.Net;
using System.Security.Cryptography;
using System.Text;

namespace Bfs.Iop.Core.UnitTests.Messaging.Webhooks;

[TestFixture(TestOf = typeof(WebhookDispatcherService))]
internal sealed class WebhookDispatcherServiceTests
{
    [Test]
    public async Task Given_queued_webhook_When_dispatched_Then_request_is_signed()
    {
        // Arrange
        var message = new WebhookMessage(Guid.NewGuid(), "CH_BAFU", AuditTrailResourceType.Dataset, Guid.NewGuid());
        var handler = new CapturingHandler();
        var httpClientFactory = Substitute.For<IHttpClientFactory>();
        httpClientFactory.CreateClient(WebhookDispatcherService.HttpClientName).Returns(_ => new HttpClient(handler));

        var queue = new ChannelMessageQueue<WebhookMessage>();
        await queue.EnqueueAsync(message);

        var service = new WebhookDispatcherService(
            queue,
            httpClientFactory,
            Options.Create(new WebhookOptions { Endpoints = { ["CH_BAFU"] = new(new("https://hooks.example/i14y"), "secret") } }),
            Substitute.For<ILogger<WebhookDispatcherService>>());

        // Act
        await service.StartAsync(CancellationToken.None);
        var (request, body) = await handler.Received.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await service.StopAsync(CancellationToken.None);

        // Assert
        var timestamp = request.Headers.GetValues("webhook-timestamp").Single();
        var expectedSignature = HMACSHA256.HashData(Encoding.UTF8.GetBytes("secret"), Encoding.UTF8.GetBytes($"{message.Id}.{timestamp}.{body}"));

        body.Should().Be($$"""{"resourceType":"Dataset","resourceId":"{{message.ResourceId}}"}""");
        request.Headers.GetValues("webhook-id").Should().Equal(message.Id.ToString());
        request.Headers.GetValues("webhook-signature").Should().Equal($"v1,{Convert.ToBase64String(expectedSignature)}");
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
