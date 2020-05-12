using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaProducer.Sample
{
    class SampleResponseProducer : BaseSampleProducer
    {
        public SampleResponseProducer(string path) : base(path)
        {
        }

        public void ProduceMessage(string key, string message)
        {
            var encryptedMessage = EncryptMessage(message) ;
            producer.Produce(new Confluent.Kafka.Message<string, string>()
            {
                Key = key,
                Value = encryptedMessage
            });
        }
    }
}
