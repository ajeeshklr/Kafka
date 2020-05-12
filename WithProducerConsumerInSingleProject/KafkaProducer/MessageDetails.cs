using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaProducer
{
    public class MessageDetails
    {
        public DateTime SentTime { get; set; }
        public string Message { get; set; }
    }
}
