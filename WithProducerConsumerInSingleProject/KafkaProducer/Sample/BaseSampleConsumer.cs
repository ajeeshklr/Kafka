using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaProducer.Sample
{
    public class BaseSampleConsumer
    {
        protected CryptoUtil cutilDecrypt;
        protected Consumer<string, string> _consumer;
        protected object lockObject = new object();

        protected Action<ConsumeResult<string, string>, BaseSampleConsumer> OnConsumed;


        public BaseSampleConsumer(string keyPath)
        {
            // cutilDecrypt = new CryptoUtil("C:\\Projects\\keys\\key.xml");
            cutilDecrypt = new CryptoUtil(keyPath);
        }

        public virtual void InitializeConsumer(string groupId, string server, string sslLocation)
        {
            var consumerConf = new ConsumerConfig
            {
                GroupId = groupId,
                BootstrapServers = server,
                StatisticsIntervalMs = 10000,
                //  QueuedMinMessages = 1000,
                //MessageMaxBytes = 25000,
                FetchWaitMaxMs = 1,
                FetchErrorBackoffMs = 10,
                EnableAutoCommit = true,
               // SocketReceiveBufferBytes = 5000000,
                SecurityProtocol = SecurityProtocol.Ssl,
                EnableSslCertificateVerification = true,
                SslCaLocation = sslLocation,
                // Debug="consumer,topic",
                AutoCommitIntervalMs = 500,
                // FetchMaxBytes = 100 *20000,
                
            };

            _consumer = new Consumer<string, string>(consumerConf, StatisticCallback);

        }

        protected virtual void StatisticCallback(IConsumer<string, string> consumer, string data)
        {
        }

        public void RegisterForConsumedCallback(Action<ConsumeResult<string, string>, BaseSampleConsumer> fn)
        {
            OnConsumed = fn;
        }

        public virtual void Subscribe(string topic)
        {
            _consumer.Consume(ConsumerResultCallback, topic);
        }

        protected virtual void ConsumerResultCallback(ConsumeResult<string, string> cr)
        {
            if(null != this.OnConsumed)
            {
                OnConsumed(cr, this);
            }
        }

        protected string DecryptEncodedData(string encodedData)
        {
            var keyvalues = encodedData.Split(new char[] { '#' });

            //Key, IV stored in first argument.
            var encryptedKeys = keyvalues[0];
            lock (lockObject)
            {

                byte[] kb = new byte[32];
                byte[] ib = new byte[16];

                var keyivs = encryptedKeys.Split(new char[] { ':' });

                var key = cutilDecrypt.Decrypt(keyivs[0]);
                var iv = cutilDecrypt.Decrypt(keyivs[1]);

                Buffer.BlockCopy(key, 0, kb, 0, 32);
                Buffer.BlockCopy(iv, 0, ib, 0, 16);

                cutilDecrypt.Key = kb;
                cutilDecrypt.IV = ib;
            }


            byte[] dataInBytes = Convert.FromBase64String(keyvalues[1]);
            return cutilDecrypt.DecryptData(dataInBytes, cutilDecrypt.Key, cutilDecrypt.IV); // This is base 64 string
        }

        public string DecryptResult(ConsumeResult<string, string> cr)
        {
            return DecryptEncodedData(cr.Message.Value);
        }
        
    }
}
