using MQTTnet.Samples.Helpers;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using System.Text;
using System.Text.Json;
using System;
using Azure.Messaging.EventHubs.Consumer;

namespace MQTTnet.Client;

public class Program
{
    private static EventHubProducerClient? eventHubClient;
    private const string EventHubConnectionString = "";
    private const string EventHubName = "";
    private const string TopicToRead = "";


    static void Main(string[] args)
    {
        MainAsync(args).GetAwaiter().GetResult();
    }

    private static async Task MainAsync(string[] args)
    {
        MqttFactory mqttFactory = new();

        using (var mqttClient = mqttFactory.CreateMqttClient())
        {
            var mqttClientOptions = new MqttClientOptionsBuilder().WithTcpServer("").Build();

            // Setup message handling before connecting so that queued messages
            // are also handled properly. When there is no event handler attached all
            // received messages get lost.
            mqttClient.ApplicationMessageReceivedAsync += async (e) =>
            {
                Console.WriteLine("Received application message.");
                Console.WriteLine();
                e.DumpToConsole();

                try
                {
                    eventHubClient = new EventHubProducerClient(EventHubConnectionString, EventHubName);

                    using EventDataBatch eventDataBatch = await eventHubClient.CreateBatchAsync();

                    EventData eventData = new(Encoding.UTF8.GetBytes(e.ApplicationMessage.ConvertPayloadToString()));

                    if (!eventDataBatch.TryAdd(eventData))
                    {
                        throw new Exception($"The message {e.ApplicationMessage} could not be added.");
                    }

                    await eventHubClient.SendAsync(eventDataBatch);
                    Console.WriteLine($"Message {eventData} sent do event hub {EventHubName}");
                    await eventHubClient.CloseAsync();

                    await ReadDataFromEh(EventHubConnectionString, EventHubName);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Some errors occured {ex.Message}");
                }
            };

            await mqttClient.ConnectAsync(mqttClientOptions, CancellationToken.None);

            var mqttSubscribeOptions = mqttFactory.CreateSubscribeOptionsBuilder()
                .WithTopicFilter(
                    f =>
                    {
                        f.WithTopic(TopicToRead);
                    })
                .Build();

            await mqttClient.SubscribeAsync(mqttSubscribeOptions, CancellationToken.None);

            Console.WriteLine("MQTT client subscribed to topic.");
            Console.WriteLine("Press enter to exit.");
            Console.ReadLine();
        }
    }

    public static async Task ReadDataFromEh(string ehConnStr, string ehName)
    {
        string consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;

        EventHubConsumerClient ehClient = new(consumerGroup, ehConnStr, ehName);

        await foreach (var eventData in ehClient.ReadEventsAsync())
        {
            var data = Encoding.UTF8.GetString(eventData.Data.Body.ToArray());
            Console.WriteLine(data);
        }

        await ehClient.CloseAsync();
    }
}

