using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;


namespace KafkaProducer
{
    public class Consumer<TKey, TValue>
    {
        IConsumer<TKey, TValue> consumer;

        Dictionary<string, IList<Action<ConsumeResult<TKey, TValue>>>> _consumers = new Dictionary<string, IList<Action<ConsumeResult<TKey, TValue>>>>();

        ConsumerConfig _config;
        public Consumer(ConsumerConfig config, Action<IConsumer<TKey, TValue>, string> statistics)
        {
            _config = config;
            var cb = new ConsumerBuilder<TKey, TValue>(config);
            if (statistics != null)
            {
                consumer = cb.SetStatisticsHandler(statistics).Build();
            }
            else consumer = cb.Build();
            ThreadPool.QueueUserWorkItem(ConsumerCallback);
        }

        private void ConsumerCallback(object state)
        {
            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };


            while (true)
            {
                try
                {
                    var c = consumer.Consume(cts.Token);
                    if (c.IsPartitionEOF)
                    {
                        Console.WriteLine($"Reached end of Topic {c.Topic}, partition {c.Partition}, offset {c.Offset}");
                        continue;
                    }
                    var msg = c.Message;
                    // Fetch all subscribers
                    IList<Action<ConsumeResult<TKey, TValue>>> subscribers;
                    if (_consumers.TryGetValue(c.Topic, out subscribers))
                    {
                        if (null != subscribers)
                        {
                            foreach (var subscriber in subscribers)
                            {
                                subscriber(c);
                            }
                        }
                    }

                    if (_config.EnableAutoCommit.Value == false)
                    {
                        consumer.Commit();
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);

                }
            }


        }

        public void Consume(Action<ConsumeResult<TKey, TValue>> subscriber, string topic)
        {
            consumer.Subscribe(topic);
            IList<Action<ConsumeResult<TKey, TValue>>> subscribers;
            if (_consumers.TryGetValue(topic, out subscribers))
            {
                if (null != subscribers)
                {
                    subscribers.Add(subscriber);
                }
            }
            else
            {
                subscribers = new List<Action<ConsumeResult<TKey, TValue>>>();
                subscribers.Add(subscriber);
                _consumers.Add(topic, subscribers);
            }

        }
    }
}
