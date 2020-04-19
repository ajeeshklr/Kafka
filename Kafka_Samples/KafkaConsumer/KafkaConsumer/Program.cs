using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaConsumer
{
    class Program
    {
        static string keys = string.Empty;

        static byte[] kb = new byte[32];
        static byte[] ib = new byte[16];
        static RSACryptoServiceProvider provider = new RSACryptoServiceProvider();
        static int blockSize = 0;
        static RijndaelManaged rijAlg = new RijndaelManaged();


        public static string DecryptData(byte[] data, byte[] key, byte[] iv)
        {
            string plaintext;



            //rijAlg.Padding = PaddingMode.None;

            // Create a decryptor to perform the stream transform.
            ICryptoTransform decryptor = rijAlg.CreateDecryptor(rijAlg.Key, rijAlg.IV);

            // Create the streams used for decryption.
            using (MemoryStream msDecrypt = new MemoryStream(data))
            {
                //msDecrypt.Position = 0;
                using (CryptoStream csDecrypt = new CryptoStream(msDecrypt, decryptor, CryptoStreamMode.Read))
                {
                    //csDecrypt.FlushFinalBlock();

                    using (StreamReader srDecrypt = new StreamReader(csDecrypt, Encoding.UTF8))
                    {
                        // Read the decrypted bytes from the decrypting stream
                        // and place them in a string.
                        plaintext = srDecrypt.ReadToEnd();
                    }
                }
            }


            return plaintext;
        }


        /// <summary>
        /// Decrypt this message using this key
        /// </summary>
        /// <param name="dataToDecrypt">
        /// The data To decrypt.
        /// </param>
        /// <param name="privateKeyInfo">
        /// The private Key Info.
        /// </param>
        /// <returns>
        /// The decrypted data.
        /// </returns>
        public static byte[] Decrypt(string str, RSACryptoServiceProvider key, int blockSize)
        {
            //// The bytearray to hold all of our data after decryption
            byte[] decryptedBytes = { };

            //Create a new instance of RSACryptoServiceProvider.
            try
            {
                byte[] bytesToDecrypt = Convert.FromBase64String(str);
                byte[] buffer = new byte[blockSize];

                //// buffer containing decrypted information
                byte[] decryptedBuffer = new byte[blockSize];

                //// Initializes our array to make sure it can hold at least the amount needed to decrypt.
                int totalBufferLength = bytesToDecrypt.Length;
                int bytesAfterDecryption = (((totalBufferLength / blockSize) - 1) * (blockSize - 32)) + blockSize; // By this calculation, we don't have to allocate too much memory.
                decryptedBytes = new byte[bytesAfterDecryption];
                int offset = 0;

                for (int i = 0; i < bytesToDecrypt.Length; i += blockSize)
                {
                    if (i + blockSize > bytesToDecrypt.Length)
                    {
                        blockSize = bytesToDecrypt.Length - i;
                        buffer = new byte[blockSize];
                    }

                    Buffer.BlockCopy(bytesToDecrypt, i, buffer, 0, blockSize);
                    decryptedBuffer = RSADecrypt(buffer, key, false);
                    decryptedBuffer.CopyTo(decryptedBytes, offset);
                    offset += decryptedBuffer.Length;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }


            return decryptedBytes;

        }

        public static byte[] RSADecrypt(byte[] DataToDecrypt, RSACryptoServiceProvider RSA, bool DoOAEPPadding)
        {
            try
            {
                return RSA.Decrypt(DataToDecrypt, DoOAEPPadding);
            }
            //Catch and display a CryptographicException  
            //to the console.
            catch (CryptographicException e)
            {
                Console.WriteLine(e.ToString());

                return null;
            }
        }

        private static long totalEllapsed = 0;
        private static int totalMessages = 0;
        private static DateTime statisticsStartTime = DateTime.Now;
        private static DateTime statisticsEndTime = DateTime.Now;
        private static DateTime lastMessageReceivedTime = DateTime.Now;
        private static int totalMessageAboveThreshold = 0;
        static bool isTestingInProgress = false;
        static long  maxDelay = 0;
        static object lockObject = new object();
        static int maxDelayThreshold = 100;

        private static void ThreadExecutionHandler(object obj)
        {
            var timeWatch = new Stopwatch();
            timeWatch.Start();

            var data = obj as String;
            // This data is in key#message format.
            var keyvalues = data.Split(new char[] { '#' });

            //Key, IV stored in first argument.
            var encryptedKeys = keyvalues[0];
            lock (lockObject)
            {
                if (!string.Equals(encryptedKeys, keys))
                {
                    var keyivs = encryptedKeys.Split(new char[] { ':' });

                    var key = Decrypt(keyivs[0], provider, blockSize);
                    var iv = Decrypt(keyivs[1], provider, blockSize);

                    Buffer.BlockCopy(key, 0, kb, 0, 32);
                    Buffer.BlockCopy(iv, 0, ib, 0, 16);
                    keys = encryptedKeys;

                    rijAlg.Key = kb;
                    rijAlg.IV = ib;
                }
            }


            byte[] dataInBytes = Convert.FromBase64String(keyvalues[1]);
            var base64String = DecryptData(dataInBytes, kb, ib);

            var actualData = Convert.FromBase64String(base64String);

            var stopWatch = new Stopwatch();
            stopWatch.Start();
            while(stopWatch.ElapsedMilliseconds < 30)   
            {
                // Simulate heavy load.
                continue;
            }
            stopWatch.Stop();
            timeWatch.Stop();
            var elapsedMilli = timeWatch.ElapsedMilliseconds;
            lock (lockObject)
            {
                totalEllapsed += elapsedMilli;
                totalMessages++;
                if (elapsedMilli > maxDelay)
                {
                    maxDelay = elapsedMilli;
                    Console.WriteLine($"Maximum delay now - {maxDelay}");
                }
            }



            if(string.Equals(base64String, "LS0tU3RhcnQgb2YgVGVzdC0tLQ=="))
            {
                statisticsStartTime = DateTime.Now;
                isTestingInProgress = true;
            }else if(string.Equals(base64String, "LS0tRW5kIG9mIFRlc3QtLS0="))
            {
                statisticsEndTime = DateTime.Now;
                isTestingInProgress = false;
            }
            if (elapsedMilli >= maxDelayThreshold)
            {
                // Console.WriteLine("Total time spent in handler - {0}ms", elapsedMilli); // Only print if elapsed is > 50ms
                lock (lockObject)
                {
                    totalMessageAboveThreshold++;
                }
            }
        }

        public static void Main(string[] args)
        {
            provider.FromXmlString(File.ReadAllText("C:\\Projects\\keys\\key.xml"));
            //// buffer to write byte sequence of the given block_size
            blockSize = (provider.KeySize / 8);

            rijAlg.Mode = CipherMode.ECB;
            rijAlg.Padding = PaddingMode.ISO10126;

            var conf = new ConsumerConfig
            {
                GroupId = "test-consumer-group",
                BootstrapServers = "localhost:9093",
                StatisticsIntervalMs = 10000,
                //  QueuedMinMessages = 1000,
                FetchWaitMaxMs = 5,
                FetchErrorBackoffMs = 10,
                EnableAutoCommit = true,
                SocketReceiveBufferBytes = 1048576,
                SecurityProtocol = SecurityProtocol.Ssl,
                EnableSslCertificateVerification = true,
                SslCaLocation = "C:\\Projects\\keys\\cert-signed",
                // Debug="consumer,topic",
                AutoCommitIntervalMs = 10000,
                
                // Note: The AutoOffsetReset property determines the start offset in the event
                // there are not yet any committed offsets for the consumer group for the
                // topic/partitions of interest. By default, offsets are committed
                // automatically, so in this example, consumption will only start from the
                // earliest message in the topic 'my-topic' the first time you run the program.
                //  AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var commitPeriod = 10; // in ms ( for manual commit operation )

            ThreadPool.SetMinThreads(8, 5);
            ThreadPool.SetMaxThreads(10, 10);

            using (var c = new ConsumerBuilder<Ignore, string>(conf)
                .SetStatisticsHandler((_, json) => {

                    if (totalMessages > 0 && !isTestingInProgress )
                    {

                        var ellapsedTime = ((statisticsEndTime < lastMessageReceivedTime ? lastMessageReceivedTime : statisticsEndTime) - statisticsStartTime).TotalMilliseconds;
                        Console.WriteLine($"Statistics: Aggregate time ellapsed - {totalEllapsed}ms, Total Messages - {totalMessages}, Average response - {totalEllapsed / totalMessages}ms");
                        Console.WriteLine($"Statistics: Total time elapsed - {ellapsedTime}, Throughput - {totalMessages / (ellapsedTime / 1000)} messages/sec.,");
                        Console.WriteLine($"Statistics: Total messages above {maxDelayThreshold}ms - {totalMessageAboveThreshold}");
                        Console.WriteLine($"Last mesasge received time - {lastMessageReceivedTime}, Statistics ended time - {statisticsEndTime}");
                        Console.WriteLine($"Maximum delay - {maxDelay}ms");

                        totalEllapsed = 0;
                        totalMessages = 0;
                        totalMessageAboveThreshold = 0;
                        maxDelay = 0;
                    }


                    })
                .Build())
            {
                c.Subscribe("my-topic");

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
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

                            var data = cr.Message.Value;

                            ThreadPool.QueueUserWorkItem(ThreadExecutionHandler, data);
                            lastMessageReceivedTime = DateTime.Now;

                            //ThreadExecutionHandler(data);
                            //  Console.WriteLine($"Consumed message at: '{cr.TopicPartitionOffset}'.");

                            //if (cr.Offset % commitPeriod == 0)
                            //{
                            //    try
                            //    {
                            //        c.Commit(cr);
                            //    }
                            //    catch (Exception ex)
                            //    {
                            //        Console.WriteLine(ex);
                            //    }
                            //}
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
