using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaProducer
{
    public class Producer<TKey, TValue>
    {
        IProducer<TKey, TValue> _producer = null;
        string producerTopic;
        public Producer(ProducerConfig config, string topic)
        {
            _producer = new ProducerBuilder<TKey, TValue>(config).Build();
            producerTopic = topic;
        }

        public void Produce(Message<TKey,TValue> message)
        {
            _producer.Produce(producerTopic, message, null);
            //_producer.Flush(TimeSpan.FromMilliseconds(1));
        } 
        
        public void Close()
        {
            _producer.Flush();
            _producer.Dispose();
        }       

    }
}
