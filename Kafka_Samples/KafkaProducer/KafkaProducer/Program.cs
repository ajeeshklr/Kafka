using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaProducer
{
    class Program
    {
        public static void Main(string[] args)
        {
            var conf = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                LingerMs = 2,
                QueueBufferingMaxKbytes = 1048576,
                 EnableSslCertificateVerification = true,
             //   Debug="msg"
                //QueueBufferingMaxMessages = 10000,
               // BatchNumMessages = 1,
            //  Acks = Acks.All
            };
            var filePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "data.dat");
            String info = File.ReadAllText(filePath);

            Action <DeliveryReport<Null, string>> handler = r =>
                Console.WriteLine(!r.Error.IsError
                    ? $"Delivered message to {r.TopicPartitionOffset}"
                    : $"Delivery Error: {r.Error.Reason}");

            using (var p = new ProducerBuilder<Null, string>(conf).Build())
            {
                for (int i = 0; i < 10000; ++i)
                {
                 
                    try
                    {
                        p.Produce("my-topic", new Message<Null, string> { Value = info }, handler);
                        if(i % 50 == 0)
                            p.Flush();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.ToString());
                        Thread.Sleep(5000);
                    }
                }

                // wait for up to 10 seconds for any inflight messages to be delivered.
                p.Flush(TimeSpan.FromSeconds(10));
            }

            Console.ReadLine();
        }
    }
}