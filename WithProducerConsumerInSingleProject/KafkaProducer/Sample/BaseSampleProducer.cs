using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaProducer.Sample
{
    public class BaseSampleProducer
    {
        protected CryptoUtil cutilEncrypt;
        protected Producer<string, string> producer;//= new Producer<string, string>(config as ProducerConfig, "my-topic");

        public BaseSampleProducer(string path)
        {
            //cutilEncrypt = new CryptoUtil("C:\\Projects\\keys\\key.xml");
            cutilEncrypt = new CryptoUtil(path);
        }

        public virtual void ConfigProducer(string topic,string server, string sslLocation)
        {
            var conf = new ProducerConfig
            {
                //BootstrapServers = "192.168.0.101:9093",
                BootstrapServers = server,
               // MessageMaxBytes = 2000,
                // BatchNumMessages = 10,
                EnableSslCertificateVerification = true,
                SecurityProtocol = SecurityProtocol.Ssl,
                //SslCaLocation = "C:\\Projects\\keys\\cert-signed",
                SslCaLocation = sslLocation,
                Acks = Acks.Leader,
                MaxInFlight = 1,
                MessageSendMaxRetries = 5,
               // SocketSendBufferBytes = 50000,
                LingerMs = 5,


            };

            // producer = new Producer<string, string>(conf, "my-topic");
            producer = new Producer<string, string>(conf, topic);
        }

        public string EncryptMessage(string messageToEncrypt)
        {
            var encryptedK = Convert.ToBase64String(cutilEncrypt.Encrypt(cutilEncrypt.Key), System.Base64FormattingOptions.None);
            var encryptedIV = Convert.ToBase64String(cutilEncrypt.Encrypt(cutilEncrypt.IV), System.Base64FormattingOptions.None);
            var messageKey = string.Format("{0}:{1}", encryptedK, encryptedIV);

            var encryptedValue = cutilEncrypt.EncryptData(messageToEncrypt);

            var message = String.Format("{0}#{1}", messageKey, Convert.ToBase64String(encryptedValue));

            return message;
        }
    }
}
