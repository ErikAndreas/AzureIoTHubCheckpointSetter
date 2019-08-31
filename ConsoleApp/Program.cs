using Microsoft.Azure.EventHubs;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Nyhren.Azure.EventHubs.CheckpointSetter
{
    // modified from https://github.com/Azure-Samples/azure-iot-samples-csharp/blob/master/iot-hub/Quickstarts/read-d2c-messages/ReadDeviceToCloudMessages.cs
    class Program
    {

        // Event Hub-compatible endpoint
        // az iot hub show --query properties.eventHubEndpoints.events.endpoint --name {your IoT Hub name}
        private static readonly string eventHubHostname;
        private readonly static string eventHubsCompatibleEndpoint;
        // Event Hub-compatible name
        // az iot hub show --query properties.eventHubEndpoints.events.path --name {your IoT Hub name}
        private readonly static string eventHubsCompatiblePath;

        // az iot hub policy show --name service --query primaryKey --hub-name {your IoT Hub name}
        private readonly static string iotHubSasKey;
        private readonly static string iotHubSasKeyName;
        private static EventHubClient eventHubClient;

        private static CloudBlobContainer cloudBlobContainer;
        private static readonly string azureStorageConnectionString;
        private static readonly string targetConsumerGroup;

        static Program()
        {
            IConfiguration config = new ConfigurationBuilder()
                  .AddJsonFile("appsettings.json", true, true)
                  .Build();
            eventHubHostname = config["Vars:EventHubHostname"];
            eventHubsCompatibleEndpoint = "sb://" + eventHubHostname;
            // Event Hub-compatible name
            // az iot hub show --query properties.eventHubEndpoints.events.path --name {your IoT Hub name}
            eventHubsCompatiblePath = config["Vars:EventHubName"];

            // az iot hub policy show --name service --query primaryKey --hub-name {your IoT Hub name}
            iotHubSasKey = config["Vars:EventHubSasKey"];
            iotHubSasKeyName = "service";

            azureStorageConnectionString = config["ConnectionStrings:BlobStorage"];
            targetConsumerGroup = config["Vars:TargetConsumerGroup"];
        }

        private static async Task ReceiveMessagesFromDeviceAsync(string partition, CancellationToken ct)
        {
            var eventHubReceiver = eventHubClient.CreateReceiver("$Default", partition, EventPosition.FromEnd());
            bool written = false;
            while (!written)
            {
                if (ct.IsCancellationRequested) break;
                Console.WriteLine("Listening for messages on: " + partition);
                var events = await eventHubReceiver.ReceiveAsync(100);

                // If there is data in the batch, process it.
                if (events == null) continue;

                foreach (EventData eventData in events)
                {
                    Console.WriteLine("Message received on partition {0} enqueued at {1} offset {2} seqno {3}:",
                        partition,
                        eventData.SystemProperties["x-opt-enqueued-time"],
                        eventData.SystemProperties["x-opt-offset"],
                        eventData.SystemProperties["x-opt-sequence-number"]);

                    var blockBlob = cloudBlobContainer.GetBlockBlobReference(partition);
                    HubCheckpointInfo input;
                    using (var stream = await blockBlob.OpenReadAsync())
                    {
                        var buff = new byte[stream.Length];

                        await stream.ReadAsync(buff, 0, (int)stream.Length);
                        string content = Encoding.UTF8.GetString(buff);
                        input = JsonConvert.DeserializeObject<HubCheckpointInfo>(content);
                        Console.WriteLine("read {0}", content);
                        input.Offset = eventData.SystemProperties["x-opt-offset"].ToString();
                        input.SequenceNumber = eventData.SystemProperties["x-opt-sequence-number"].ToString();
                    }
                    string output = JsonConvert.SerializeObject(input);
                    using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(output), false))
                    {
                        blockBlob.UploadFromStream(stream, null);
                    }
                    Console.WriteLine("wrote {0}", output);
                    written = true;
                    break;
                }
            }
        }

        static async Task Main(string[] args)
        {
            Console.WriteLine("IoT Hub Quickstarts - Read device to cloud messages. Ctrl-C to exit.\n");

            // Create an EventHubClient instance to connect to the
            // IoT Hub Event Hubs-compatible endpoint.
            var connectionString = new EventHubsConnectionStringBuilder(new Uri(eventHubsCompatibleEndpoint), eventHubsCompatiblePath, iotHubSasKeyName, iotHubSasKey);
            eventHubClient = EventHubClient.CreateFromConnectionString(connectionString.ToString());

            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(azureStorageConnectionString);
            var blobClient = storageAccount.CreateCloudBlobClient();
            cloudBlobContainer = blobClient.GetContainerReference("azure-webjobs-eventhub/" + eventHubHostname + "/" + eventHubsCompatiblePath + "/" + targetConsumerGroup);

            // Create a PartitionReciever for each partition on the hub.
            var runtimeInfo = await eventHubClient.GetRuntimeInformationAsync();
            var d2cPartitions = runtimeInfo.PartitionIds;

            CancellationTokenSource cts = new CancellationTokenSource();

            Console.CancelKeyPress += (s, e) =>
                {
                    e.Cancel = true;
                    cts.Cancel();
                    Console.WriteLine("Exiting...");
                };

            var tasks = new List<Task>();
            foreach (string partition in d2cPartitions)
            {
                tasks.Add(ReceiveMessagesFromDeviceAsync(partition, cts.Token));
            }

            // Wait for all the PartitionReceivers to finsih.
            Task.WaitAll(tasks.ToArray());
        }
    }

    class HubCheckpointInfo
    {
        public string Offset { get; set; }
        public string SequenceNumber { get; set; }
        public string PartitionId { get; set; }
        public string Owner { get; set; }
        public string Token { get; set; }
        public string Epoch { get; set; }
    }
}
