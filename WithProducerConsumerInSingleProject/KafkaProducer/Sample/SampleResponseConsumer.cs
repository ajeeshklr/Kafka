using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace KafkaProducer.Sample
{
    public class SampleResponseConsumer : BaseSampleConsumer
    {
        public SampleResponseConsumer(string keyPath) : base(keyPath)
        {
        }

        protected override void ConsumerResultCallback(ConsumeResult<string, string> cr)
        {
            base.ConsumerResultCallback(cr);
        }
    }
}
