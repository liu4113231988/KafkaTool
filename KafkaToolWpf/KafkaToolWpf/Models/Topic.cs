using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaToolWpf.Models
{
    public class Topic
    {
        public string TopicName { get; set; }

        public Topic(string topicName)
        {
            TopicName = topicName;
        }
    }
}
