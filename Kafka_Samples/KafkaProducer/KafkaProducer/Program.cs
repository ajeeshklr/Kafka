using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaProducer
{
    class Program
    {
        static RijndaelManaged rijAlg = null;
        static ICryptoTransform encryptor = null;

        public static byte[] EncryptData(string data)
        {
            byte[] encrypted;
            // Create an RijndaelManaged object
            // with the specified key and IV.

            // Create the streams used for encryption.
            using (MemoryStream msEncrypt = new MemoryStream())
            {
                using (CryptoStream csEncrypt = new CryptoStream(msEncrypt, encryptor, CryptoStreamMode.Write))
                {
                    using (StreamWriter swEncrypt = new StreamWriter(csEncrypt, Encoding.UTF8))
                    {
                        //Write all data to the stream.
                        swEncrypt.Write(data);
                        swEncrypt.Close();
                    }
                    encrypted = msEncrypt.ToArray();
                }
                msEncrypt.Close();
            }

            return encrypted;
        }

        public static byte[] RSAEncrypt(byte[] DataToEncrypt, RSAParameters RSAKeyInfo, bool DoOAEPPadding)
        {
            try
            {
                byte[] encryptedData;
                //Create a new instance of RSACryptoServiceProvider.
                using (RSACryptoServiceProvider RSA = new RSACryptoServiceProvider())
                {

                    //Import the RSA Key information. This only needs
                    //toinclude the public key information.
                    RSA.ImportParameters(RSAKeyInfo);

                    //Encrypt the passed byte array and specify OAEP padding.  
                    //OAEP padding is only available on Microsoft Windows XP or
                    //later.  
                    encryptedData = RSA.Encrypt(DataToEncrypt, DoOAEPPadding);
                }
                return encryptedData;
            }
            //Catch and display a CryptographicException  
            //to the console.
            catch (CryptographicException e)
            {
                Console.WriteLine(e.Message);

                return null;
            }
        }

        public static byte[] Encrypt(byte[] encryptThis, RSACryptoServiceProvider provider)
        {
            try
            {

                int encryptedBufferSize = (provider.KeySize / 8);
                int blockSize = encryptedBufferSize - 32;

                //// buffer to write byte sequence of the given block_size
                byte[] buffer = new byte[blockSize];

                byte[] encryptedBuffer = new byte[encryptedBufferSize];

                var totalBufferLength = ((encryptThis.Length / blockSize) * encryptedBufferSize) + ((encryptThis.Length % blockSize) > 0 ? encryptedBufferSize : 0);


                //// Initializing our encryptedBytes array to a suitable size, depending on the size of data to be encrypted
                byte[] encryptedBytes = new byte[totalBufferLength];
                int offset = 0;

                for (int i = 0; i < encryptThis.Length; i += blockSize)
                {
                    if (i + blockSize > encryptThis.Length)
                    {
                        blockSize = encryptThis.Length - i;
                        buffer = new byte[blockSize];
                    }

                    //// encrypt the specified size of data, then add to final array.
                    Buffer.BlockCopy(encryptThis, i, buffer, 0, blockSize);
                    encryptedBuffer = RSAEncrypt(buffer, provider.ExportParameters(false), false);
                    encryptedBuffer.CopyTo(encryptedBytes, offset);
                    offset += encryptedBufferSize;
                }

                return encryptedBytes;
            }
            catch (CryptographicException e)
            {
                Console.WriteLine(e);
                return null;
            }
        }

        public static void Main(string[] args)
        {
            var provider = new RSACryptoServiceProvider();
            provider.FromXmlString(File.ReadAllText("C:\\Projects\\keys\\key.xml"));


            var conf = new ProducerConfig
            {
                BootstrapServers = "localhost:9093",
                LingerMs = 10,
                QueueBufferingMaxKbytes = 1048576,
                MessageMaxBytes = 50 * 1024,
                EnableSslCertificateVerification = true,
                SecurityProtocol = SecurityProtocol.Ssl,
                SslCaLocation = "C:\\Projects\\keys\\cert-signed",
                Partitioner = Partitioner.Consistent,
                //SslCertificateLocation = "C:\\Projects\\kafka_repo\\keys\\pub.pem",
                //SslKeyLocation= "C:\\Projects\\kafka_repo\\keys\\pri.key",
                //SslKeyPassword = "password123",
                //   Debug="msg"
                //QueueBufferingMaxMessages = 10000,
                // BatchNumMessages = 1,
                //  Acks = Acks.All
            };
            var filePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "data.dat");
            //byte[] info = Encoding.ASCII.GetBytes("Ajeesh B Nair");
            //byte[] info = Encoding.UTF8.GetBytes("Ajeesh B Nair asdddddddddddddddddddddddddddddddddddddddddddd asddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddassssssssqwdqw-Ajeesh B Nair asdddddddddddddddddddddddddddddddddddddddddddd asddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddassssssssqwdqw");//  File.ReadAllBytes(filePath);
            byte[] info = File.ReadAllBytes(filePath);
            var startMessage = "---Start of Test---";
            var endingMessage = "---End of Test---";
            var startMessageBytes = Encoding.ASCII.GetBytes(startMessage);
            var endMessageBytes = Encoding.ASCII.GetBytes(endingMessage);

            var b64StartMessage = Convert.ToBase64String(startMessageBytes);
            var b64EndMessage = Convert.ToBase64String(endMessageBytes);

            var base64String = Convert.ToBase64String(info);

            rijAlg = new RijndaelManaged();
            rijAlg.Mode = CipherMode.ECB;
            rijAlg.Padding = PaddingMode.ISO10126;

            encryptor = rijAlg.CreateEncryptor(rijAlg.Key, rijAlg.IV);

            var key = rijAlg.Key;
            var iv = rijAlg.IV;


            // Encrypt key and iv using RSA
            var encryptedK = Convert.ToBase64String(Encrypt(key, provider), System.Base64FormattingOptions.None);
            var encryptedIV = Convert.ToBase64String(Encrypt(iv, provider), System.Base64FormattingOptions.None);

            var messageKey = string.Format("{0}:{1}", encryptedK, encryptedIV);



            Action<DeliveryReport<Null, string>> handler = r =>
           Console.WriteLine(!r.Error.IsError
               ? $"Delivered message to {r.TopicPartitionOffset}"
               : $"Delivery Error: {r.Error.Reason}");

            using (var p = new ProducerBuilder<Null, string>(conf).Build())
            {
                var startTime = DateTime.Now.Ticks;
                
                for (int i = 0; i < 10000; ++i)
                {
                    try
                    {
                        // Encrypt data using AES

                        string dataToEncrypt;
                        if (i == 0)
                        {
                            dataToEncrypt = b64StartMessage;
                        }
                        else if (i == 9999)
                        {
                            dataToEncrypt = b64EndMessage;
                        }
                        else
                        {
                            dataToEncrypt = base64String;
                        }
                        var encryptedValue = EncryptData(dataToEncrypt);
                        var message = String.Format("{0}#{1}", messageKey, Convert.ToBase64String(encryptedValue));
                        p.Produce("my-topic", new Message<Null, string> { Value = message }, null);

                        if (i % 49 == 0)
                        {
                            p.Flush();
                            Thread.Sleep(100);
                        }

                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.ToString());
                        Thread.Sleep(5000);
                    }
                }

                var endTime = DateTime.Now.Ticks;
                var totalTimeTaken = (endTime - startTime) / TimeSpan.TicksPerMillisecond;
                Console.WriteLine("Total time took to produce 10000 messages is {0}ms\r\n", totalTimeTaken);

                // wait for up to 10 seconds for any inflight messages to be delivered.
                p.Flush(TimeSpan.FromSeconds(10));
            }

            Console.ReadLine();
        }
    }
}