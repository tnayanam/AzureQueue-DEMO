
using System;
using System.IO;
using System.Text;
using Newtonsoft.Json;
using Amazon.Lambda.Core;
using Amazon.Lambda.DynamoDBEvents;
using Amazon.DynamoDBv2.Model;
using Microsoft.Azure.ServiceBus;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace corona
{
    public class Function
    {
        private JsonSerializer _jsonSerializer = new JsonSerializer();
        private const string connectionString = "QUEUE_NAME";
        private const string queueName = "dynamoqueue";
        private readonly IQueueClient client;
        public Function()
        {
            ServiceBusConnectionStringBuilder svc = new ServiceBusConnectionStringBuilder(connectionString);
            ServiceBusConnection svc1 = new ServiceBusConnection(svc);
            client = new QueueClient(svc1, queueName, ReceiveMode.PeekLock, RetryPolicy.Default);
        }

        public async Task FunctionHandler(DynamoDBEvent dynamoEvent, ILambdaContext context)
        {
            context.Logger.LogLine($"Beginning to process {dynamoEvent.Records.Count} records...");

            try
            {
                foreach (var record in dynamoEvent.Records)
                {
                    try
                    {
                        if (record.EventName == OperationType.REMOVE)
                        {
                            context.Logger.LogLine($"Event ID: {record.EventID}");
                            context.Logger.LogLine($"Event Name: {record.EventName}");
                            context.Logger.LogLine("Calling SerializeStreamRecord function");
                            string streamRecordJson = SerializeStreamRecord(record.Dynamodb);
                            context.Logger.LogLine("SerializeStreamRecord Completed");
                            context.Logger.LogLine("Calling Azure");
                            await SendAsync(streamRecordJson, context).ConfigureAwait(false);
                            await client.CloseAsync().ConfigureAwait(false);
                            context.Logger.LogLine("Calling Azure Completed");
                            context.Logger.LogLine($"DynamoDB Record:");
                            context.Logger.LogLine(streamRecordJson);
                            context.Logger.LogLine("Data Sent");
                        }
                    }
                    catch (Exception ex)
                    {
                        throw ex;
                    }
                }
            }
            catch (Exception ex)
            {
                context.Logger.LogLine("Exception Occurred" + ex.Message);
                context.Logger.LogLine("Inner Exception Occurred" + ex.InnerException);
            }

            context.Logger.LogLine("Stream processing complete.");
        }

        private async Task SendAsync(string stream, ILambdaContext context)
        {
            try
            {
                context.Logger.LogLine("Starting to send");
                var message = new Message(Encoding.UTF8.GetBytes(stream));
                context.Logger.LogLine("Message Ready");
                await client.SendAsync(message).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }

        private string SerializeStreamRecord(StreamRecord streamRecord)
        {
            try
            {
                using (var writer = new StringWriter())
                {
                    _jsonSerializer.Serialize(writer, streamRecord);
                    return writer.ToString();
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }
    }
}