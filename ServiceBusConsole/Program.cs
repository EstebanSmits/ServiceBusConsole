using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Newtonsoft.Json;


namespace ServiceBusConsole
{

   
    class Program
    {
        // Connection String for the namespace can be obtained from the Azure portal under the 
        // 'Shared Access policies' section.
        const string ServiceBusConnectionString = "Endpoint=sb://YOURSERVICESBUSS.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=YOURKEY";
        public static string queueName = "default";
        
        static IQueueClient queueClient;
        static dynamic data;
        static void Main(string[] args)
        {
            
            data = new[]
            {
                new {name = "Einstein", firstName = "Albert"},
                new {name = "Heisenberg", firstName = "Werner"},
                new {name = "Curie", firstName = "Marie"},
                new {name = "Hawking", firstName = "Steven"},
                new {name = "Newton", firstName = "Isaac"},
                new {name = "Bohr", firstName = "Niels"},
                new {name = "Faraday", firstName = "Michael"},
                new {name = "Galilei", firstName = "Galileo"},
                new {name = "Kepler", firstName = "Johannes"},
                new {name = "Kopernikus", firstName = "Nikolaus"}
            };
            MainAsync().GetAwaiter().GetResult();
        }

        static async Task MainAsync()
        {
            const int numberOfMessages = 10;
            //queueClient = new QueueClient(ServiceBusConnectionString,  EntityNameHelper.FormatDeadLetterPath(queueName));
                 queueClient = new QueueClient(ServiceBusConnectionString, queueName);

            Console.WriteLine("======================================================");
            Console.WriteLine(" Press 1 to send message,  2 to schedule messages, " +
                              "3 to recieve/abandon messages and 4 to recieve/complete messages");
            Console.WriteLine("======================================================");
            var keyInput =Console.ReadKey();
            switch (keyInput.Key)
            {
                case ConsoleKey.D1:
                    await SendMessagesAsync(numberOfMessages,false);
                    break;
                case ConsoleKey.D2:
                    await SendMessagesAsync(numberOfMessages, true);
                    break;
                case ConsoleKey.D3:
                    RegisterOnMessageHandlerAndReceiveMessages(false);
                    break;
                case ConsoleKey.D4:
                    RegisterOnMessageHandlerAndReceiveMessages(true);
                    break;

            }
            Console.ReadKey();
            await queueClient.CloseAsync();

            

        }

        static async Task SendMessagesAsync(int numberOfMessagesToSend,bool sendScheduled)
        {
            try
            {
                for (var i = 0; i < Enumerable.Count(data); i++)
                {
                    // Create a new message to send to the queue
                    //string messageBody = $"Message {i}";
                    var message = new Message(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(data[i])));

                    // Write the body of the message to the console
                    Console.WriteLine($"Sending message: {Encoding.UTF8.GetString(message.Body)}");

                    // Send the message to the queue
                    if (sendScheduled)
                    {
                        await queueClient.ScheduleMessageAsync(message, DateTime.Now.AddDays(1));

                    }
                    else
                    {
                        await queueClient.SendAsync(message);
                    }
                    
                    
                }
            }
            catch (Exception exception)
            {
                Console.WriteLine($"{DateTime.Now} :: Exception: {exception.Message}");
            }
        }

   
        static void RegisterOnMessageHandlerAndReceiveMessages(bool clearmessage)
        {
            // Configure the MessageHandler Options in terms of exception handling, number of concurrent messages to deliver etc.
            var messageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler)
            {
                // Maximum number of Concurrent calls to the callback `ProcessMessagesAsync`, set to 1 for simplicity.
                // Set it according to how many messages the application wants to process in parallel.
                MaxConcurrentCalls = 5,

                // Indicates whether MessagePump should automatically complete the messages after returning from User Callback.
                // False below indicates the Complete will be handled by the User Callback as in `ProcessMessagesAsync` below.
                AutoComplete = false,
            };

            // Register the function that will process messages
            if (clearmessage) { 
                queueClient.RegisterMessageHandler(ProcessMessagesAsync, messageHandlerOptions);
            }
            else { 
                queueClient.RegisterMessageHandler(ProcessMessagesAndAbandonAsync, messageHandlerOptions);
            }
        }

        static async Task ProcessMessagesAsync(Message message, CancellationToken token)
        {
            // Process the message
            Console.WriteLine($"Received message: SequenceNumber:{message.SystemProperties.SequenceNumber}, " +
                              $"Delivery Count: {message.SystemProperties.DeliveryCount} " +
                              $"Body:{Encoding.UTF8.GetString(message.Body)}" +
                              $"Sequence Number : {message.SystemProperties.EnqueuedSequenceNumber}" +
                              $" DeadLetter Source:  {message.SystemProperties.DeadLetterSource}");

            // Complete the message so that it is not received again.
            // This can be done only if the queueClient is created in ReceiveMode.PeekLock mode (which is default).
           await queueClient.CompleteAsync(message.SystemProperties.LockToken);
            

            // Note: Use the cancellationToken passed as necessary to determine if the queueClient has already been closed.
            // If queueClient has already been Closed, you may chose to not call CompleteAsync() or AbandonAsync() etc. calls 
            // to avoid unnecessary exceptions.
        }


        static async Task ProcessMessagesAndAbandonAsync(Message message, CancellationToken token)
        {
            // Process the message
            Console.WriteLine($"Received message: SequenceNumber:{message.SystemProperties.SequenceNumber}, Delivery Count: {message.SystemProperties.DeliveryCount} Body:{Encoding.UTF8.GetString(message.Body)}");

            // Complete the message so that it is not received again.
            // This can be done only if the queueClient is created in ReceiveMode.PeekLock mode (which is default).
            await queueClient.AbandonAsync(message.SystemProperties.LockToken);
            // Note: Use the cancellationToken passed as necessary to determine if the queueClient has already been closed.
            // If queueClient has already been Closed, you may chose to not call CompleteAsync() or AbandonAsync() etc. calls 
            // to avoid unnecessary exceptions.
        }

        static Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            Console.WriteLine($"Message handler encountered an exception {exceptionReceivedEventArgs.Exception}.");
            var context = exceptionReceivedEventArgs.ExceptionReceivedContext;
            Console.WriteLine("Exception context for troubleshooting:");
            Console.WriteLine($"- Endpoint: {context.Endpoint}");
            Console.WriteLine($"- Entity Path: {context.EntityPath}");
            Console.WriteLine($"- Executing Action: {context.Action}");
            return Task.CompletedTask;
        }

    }


}