using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaProducer.Sample
{
    public class SampleProducer : BaseSampleProducer
    {
        private int producerDelay = 0;
        private int messagesPerSecond = 2;
        private Dictionary<string, MessageDetails> messages = new Dictionary<string, MessageDetails>();

        public SampleProducer(String keyPath) : base(keyPath)
        {
        }

        private void ProducerRoutine(object config)
        {
            var filePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "data.dat");
            byte[] info = File.ReadAllBytes(filePath);
            var startMessage = "---Start of Test---";
            var endingMessage = "---End of Test---";
            var startMessageBytes = Encoding.ASCII.GetBytes(startMessage);
            var endMessageBytes = Encoding.ASCII.GetBytes(endingMessage);

            var b64StartMessage = Convert.ToBase64String(startMessageBytes);
            var b64EndMessage = Convert.ToBase64String(endMessageBytes);

            var base64String = Convert.ToBase64String(info);

            var key = cutilEncrypt.Key;
            var iv = cutilEncrypt.IV;

            Action<DeliveryReport<string, string>> handler = r =>
           Console.WriteLine(!r.Error.IsError
               ? $"Delivered message to {r.TopicPartitionOffset}"
               : $"Delivery Error: {r.Error.Reason}");
            var count = 10;

            var startTime = DateTime.Now.Ticks;
            producerDelay = messagesPerSecond > 0 ? 1000 / messagesPerSecond : 0;

            for (int i = 0; i < count; ++i)
            {
                try
                {
                    var timeInUTC = DateTime.UtcNow;
                    // Encrypt data using AES

                    string dataToEncrypt;
                    if (i == 0)
                    {
                        dataToEncrypt = string.Format("{0}#{1}", b64StartMessage, timeInUTC.ToFileTimeUtc()) ;
                    }
                    else if (i == count - 1)
                    {
                        dataToEncrypt = string.Format("{0}#{1}", b64EndMessage, timeInUTC.ToFileTimeUtc());
                    }
                    else
                    {
                        dataToEncrypt = string.Format("{0}#{1}", base64String, timeInUTC.ToFileTimeUtc());
                    }
                    //var encryptedValue = cutilEncrypt.EncryptData(dataToEncrypt);
                    var message = EncryptMessage(dataToEncrypt);//  String.Format("{0}#{1}", messageKey, Convert.ToBase64String(encryptedValue));

                    var msg = new MessageDetails()
                    {
                        Message = message,
                        SentTime = DateTime.UtcNow
                    };
                    var uid = Guid.NewGuid().ToString();
                    messages.Add(uid, msg);
                    var m = new Message<string, string>() {
                        Key = uid,
                        Value = message,
                        Timestamp = new Timestamp(timeInUTC)
                    };

                    producer.Produce(m);

                    Thread.Sleep(producerDelay);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                    Thread.Sleep(5000);
                }
            }

            var endTime = DateTime.Now.Ticks;
            var totalTimeTaken = (endTime - startTime) / TimeSpan.TicksPerMillisecond;
            Console.WriteLine("Total time took to produce {0} messages is {1}ms\r\n", count, totalTimeTaken);

        }

        public override void ConfigProducer(string topic, string server, string sslLocation)
        {
            base.ConfigProducer(topic, server, sslLocation);

        }

        public void ProduceSampleMessages()
        {
            ThreadPool.QueueUserWorkItem(ProducerRoutine);
        }

        private void DeliveryReport(DeliveryReport<string, string> obj)
        {
            MessageDetails detail = null;
            if (messages.TryGetValue(obj.Key, out detail))
            {
                Console.WriteLine("Total time to transmit message - {0} is  {1} - {2} ", obj.Key, obj.Timestamp.Type, (DateTime.UtcNow - detail.SentTime).TotalMilliseconds);
            }
        }
    }
}
