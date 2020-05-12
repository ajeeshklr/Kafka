using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaProducer.Sample
{
    public class SampleConsumer : BaseSampleConsumer
    {

        private static long totalEllapsed = 0;
        private static int totalMessages = 0;
        private static DateTime statisticsStartTime = DateTime.Now;
        private static DateTime statisticsEndTime = DateTime.Now;
        private static DateTime lastMessageReceivedTime = DateTime.Now;
        private static int totalMessageAboveThreshold = 0;
        static bool isTestingInProgress = false;
        static long maxDelay = 0;
        static int maxDelayThreshold = 100;


        public SampleConsumer(string path) : base(path)
        {
            
        }

        public override void InitializeConsumer(string groupId, string server, string sslLocation)
        {
            base.InitializeConsumer(groupId, server, sslLocation);
        }
        
        private void ThreadExecutionHandler(object obj)
        {
            var cr = obj as ConsumeResult<string, string>;

            var timeWatch = new Stopwatch();
            timeWatch.Start();

            var data = cr.Message.Value;

            var base64String = DecryptEncodedData(data);                
            var values = base64String.Split("#".ToCharArray());
            var actualData = Convert.FromBase64String(values[0]);// Convert.FromBase64String(base64String);

            var stopWatch = new Stopwatch();
            stopWatch.Start();
            while (stopWatch.ElapsedMilliseconds < 80)
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

            if (string.Equals(base64String, "LS0tU3RhcnQgb2YgVGVzdC0tLQ=="))
            {
                statisticsStartTime = DateTime.Now;
                isTestingInProgress = true;
            }
            else if (string.Equals(base64String, "LS0tRW5kIG9mIFRlc3QtLS0="))
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

            OnConsumed?.Invoke(cr, this);
        }

        protected override void ConsumerResultCallback(ConsumeResult<string, string> cr)
        {
            lastMessageReceivedTime = DateTime.Now;
            // base.ConsumerResultCallback(cr); // Don't call base, instead process the mesasge

            ThreadPool.QueueUserWorkItem(ThreadExecutionHandler, cr);

        }

        protected override void StatisticCallback(IConsumer<string, string> consumer, string data)
        {
            base.StatisticCallback(consumer, data);

            if (totalMessages > 0 && !isTestingInProgress)
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
        }

    }
}
