using Bfs.Iop.Common.Messaging;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace Bfs.Iop.Core.Messaging.Webhooks;

internal sealed class WebhookDispatcherService : BackgroundService
{
    public const string HttpClientName = "Webhooks";

    private const int MaxRetriesInCaseOfFail = 10;
    private const int TimeBetweenRetriesinMs = 300;

    private readonly IMessageQueue<WebhookMessage> _queue;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly WebhookOptions _options;
    private readonly ILogger<WebhookDispatcherService> _logger;

    public WebhookDispatcherService(
        IMessageQueue<WebhookMessage> queue,
        IHttpClientFactory httpClientFactory,
        IOptions<WebhookOptions> options,
        ILogger<WebhookDispatcherService> logger)
    {
        _queue = queue;
        _httpClientFactory = httpClientFactory;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await foreach (var message in _queue.DequeueAllAsync(stoppingToken))
        {
            var endpoint = _options.Endpoints[message.AgentIdentifier];

            try
            {
                using var request = CreateRequest(message, endpoint);
                using var response = await _httpClientFactory.CreateClient(HttpClientName).SendAsync(request, stoppingToken);

                response.EnsureSuccessStatusCode();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Webhook {MessageId} to {Host} failed. Retry {RetryCount}.", message.Id, endpoint.Url.Host, message.FailCount);

                if (message.FailCount < MaxRetriesInCaseOfFail)
                {
                    var retryMessage = message with
                    {
                        FailCount = message.FailCount + 1
                    };

                    await Task.Delay(TimeBetweenRetriesinMs);

                    await _queue.EnqueueAsync(retryMessage, stoppingToken);
                }
            }
        }
    }

    private static HttpRequestMessage CreateRequest(WebhookMessage message, WebhookEndpoint endpoint)
    {
        var body = JsonSerializer.Serialize(new { resourceType = message.ResourceType.ToString(), resourceId = message.ResourceId });
        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture);
        var signature = HMACSHA256.HashData(
            Encoding.UTF8.GetBytes(endpoint.Secret),
            Encoding.UTF8.GetBytes($"{message.Id}.{timestamp}.{body}"));

        var request = new HttpRequestMessage(HttpMethod.Post, endpoint.Url)
        {
            Content = new StringContent(body, Encoding.UTF8, "application/json")
        };

        request.Headers.Add("webhook-id", message.Id.ToString());
        request.Headers.Add("webhook-timestamp", timestamp);
        request.Headers.Add("webhook-signature", $"v1,{Convert.ToBase64String(signature)}");

        return request;
    }
}
