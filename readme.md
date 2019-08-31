# Azure IoT/EventHub Checkpoint Setter
## What
Dotnet Core console app which connects to Azure IotHub (EventHub should work the same?) and reads Checkpoint/Offset info from the last message in the Hub and writes that (Offset and SequenceNumber) to the targeted azure-webjobs-eventhub blob container.
## Why
When consuming messages in an Azure IoT- or EventHub from an Azure EventHubTriggered Function you got [no means](https://github.com/Azure/azure-webjobs-sdk/issues/1240) to indicate from where you want to start consuming your messages. If your Trigger Function "falls behind" and messages are added to hub with a high frequence you might never catch up and spend compute time (and money) on cathing up (on stuff you might not be interested in if you only want the latest).
## Notes
* This program could probably cause a lot of mess if/when not working properly, use at own risk!
* This program is hopefully not needed when [fixed](https://github.com/Azure/azure-webjobs-sdk/issues/1240) in Azure Webjobs SDK and support is added (function attributes bindings?)
* The function host using _targetConsumerGroup_ and its underlying storage should not be running while this program executes.
## How
* Clone this repo
* Populate appsetting.json with your config
* Actually read what the code does (again, use at your own risk)
* Build and run!
## appsettings.json
Sample for use with local storage emulator (Note, storage emulator seems to be discontinued and support only Storage SDK < 11 (i.e currently 10.0.3))
```json
{
  "ConnectionStrings": {
    "BlobStorage": "UseDevelopmentStorage=true"
  },
  "Vars": {
    "EventHubHostname": "...",
    "EventHubName": "...",
    "EventHubSasKey": "...",
    "TargetConsumerGroup": "..."
  }
}
```
