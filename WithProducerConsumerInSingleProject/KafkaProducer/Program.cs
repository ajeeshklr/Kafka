using Confluent.Kafka;
using KafkaProducer.Sample;
using System;
using System.Threading;

namespace KafkaProducer
{
    class Program
    {

        static SampleResponseProducer responseProducer;
        public static void Main(string[] args)
        {
            bool runConsumer = false;
            bool runProducer = false;


            // Check command line arguments
            if(args.Length > 0)
            {
                if(args[0] == "p")
                {
                    runProducer = true;
                } else if(args[0] == "c")
                {
                    runConsumer = true;
                }else if(args[0] == "a")
                {
                    runProducer = true;
                    runConsumer = true;
                }
            }

            //ThreadPool.SetMaxThreads(20, 5); 
            // Note: The AutoOffsetReset property determines the start offset in the event
            // there are not yet any committed offsets for the consumer group for the
            // topic/partitions of interest. By default, offsets are committed
            // automatically, so in this example, consumption will only start from the
            // earliest message in the topic 'my-topic' the first time you run the program.

            if (runConsumer)
            {
                SampleConsumer consumer = new SampleConsumer("C:\\Projects\\keys\\key.xml");
                consumer.InitializeConsumer("test-consumer-group", "192.168.0.101:9093", "C:\\Projects\\keys\\cert-signed");
                consumer.Subscribe("my-topic");
                consumer.RegisterForConsumedCallback(OnResponseFromConsumerReceived);

                // Need to create a producer to send the response back.
                responseProducer = new SampleResponseProducer("C:\\Projects\\keys\\key.xml");
                responseProducer.ConfigProducer("response-topic", "192.168.0.101:9093", "C:\\Projects\\keys\\cert-signed");

            }

            if (runProducer)
            {
                SampleProducer sampleProducer = new SampleProducer("C:\\Projects\\keys\\key.xml");
                sampleProducer.ConfigProducer("my-topic", "192.168.0.101:9093", "C:\\Projects\\keys\\cert-signed");
                sampleProducer.ProduceSampleMessages();

                SampleResponseConsumer responseConsumer = new SampleResponseConsumer("C:\\Projects\\keys\\key.xml");
                responseConsumer.InitializeConsumer("test-response-consumer-group", "192.168.0.101:9093", "C:\\Projects\\keys\\cert-signed");
                responseConsumer.Subscribe("response-topic");
                responseConsumer.RegisterForConsumedCallback(OnResultAtProducerReceived);

                while (true)
                {
                    if(Console.Read() == 'r')
                    sampleProducer.ProduceSampleMessages();
                }

            }

            Console.ReadLine();
        }

        private static void OnResponseFromConsumerReceived(ConsumeResult<string,string> cr, BaseSampleConsumer consumer)
        {
            var fileTime = cr.Message.Timestamp.UtcDateTime.ToFileTimeUtc();
            // Create a sample response and send to the originator of the message.
            Console.WriteLine($"Message received at OnResponseFromConsumerReceived. Created time for {cr.Message.Key} is {cr.Message.Timestamp.UtcDateTime.ToFileTimeUtc()}");

            responseProducer.ProduceMessage(cr.Message.Key, cr.Message.Timestamp.UtcDateTime.ToFileTimeUtc().ToString());

        }
        private static void OnResultAtProducerReceived(ConsumeResult<string,string> cr, BaseSampleConsumer consumer)
        {
            var value = consumer.DecryptResult(cr); // File time UTC
            Console.WriteLine($"OnResultAtProducerReceived for {cr.Message.Key}. Created Time - {value}");
            // Let's print this value in console.
            // It should say how mutch time it took for processing the message.
            long time = long.Parse( value );

            DateTime createdTime = DateTime.FromFileTimeUtc(time);
            var diff = (DateTime.UtcNow - createdTime).TotalMilliseconds;
            Console.WriteLine($"Total time took for the message {cr.Message.Key} is {diff}");


        }




    }
}