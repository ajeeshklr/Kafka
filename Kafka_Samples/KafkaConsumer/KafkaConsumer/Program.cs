using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaConsumer
{
    class Program
    {
        public static void Main(string[] args)
        {
            var conf = new ConsumerConfig
            {
                GroupId = "test-consumer-group",
                BootstrapServers = "localhost:9092",
                StatisticsIntervalMs = 1200000,
              //  QueuedMinMessages = 1000,
                FetchWaitMaxMs = 5,
                FetchErrorBackoffMs = 10,
                EnableAutoCommit = false,
                SocketReceiveBufferBytes = 1048576,
               // Debug="consumer,topic",
               // AutoCommitIntervalMs = 2,
                // Note: The AutoOffsetReset property determines the start offset in the event
                // there are not yet any committed offsets for the consumer group for the
                // topic/partitions of interest. By default, offsets are committed
                // automatically, so in this example, consumption will only start from the
                // earliest message in the topic 'my-topic' the first time you run the program.
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var commitPeriod = 5; // in ms ( for manual commit operation )

            using (var c = new ConsumerBuilder<Ignore, string>(conf)
                .SetStatisticsHandler((_, json) => Console.WriteLine($"Statistics: {json}"))
                .Build())
            {
                c.Subscribe("my-topic");

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = c.Consume(cts.Token);
                            if (cr.IsPartitionEOF)
                            {
                                Console.WriteLine(
                                    $"Reached end of topic {cr.Topic}, partition {cr.Partition}, offset {cr.Offset}.");

                                continue;
                            }

                            Console.WriteLine($"Consumed message at: '{cr.TopicPartitionOffset}'.");

                            if (cr.Offset % commitPeriod == 0)
                            {
                                try
                                {
                                    c.Commit(cr);
                                }catch(Exception ex)
                                {
                                    Console.WriteLine(ex);
                                }
                            }
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Ensure the consumer leaves the group cleanly and final offsets are committed.
                    c.Close();
                }
            }
        }
    }
}
