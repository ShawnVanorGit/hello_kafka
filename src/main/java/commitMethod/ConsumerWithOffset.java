package commitMethod;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @Author Natasha
 * @Description
 * @Date 2020/11/3 14:14
 **/

public class ConsumerWithOffset {

    public static void main(String[] args)  {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "120.27.233.226:9092");
        properties.put("group.id", "test");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 把auto.commit.offset设为false，让应用程序决定何时提交偏移量
        properties.put("auto.commit.offset", false);
        properties.put("auto.commit.interval.ms", "5000");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singletonList("test"));
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap();
        int count = 0;

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("value = " + record.value() + ", topic = " + record.topic() + ", partition = " + record.partition() + ", offset = " + record.offset());
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1, "no metadata"));
                    if (count % 1000 == 0) {
                        // 这里调用的是 commitAsync()，不过调用 commitSync() 也是完全可以的
                        // 当然，在提交特定偏移量时，仍然要处理可能发生的错误
                        consumer.commitAsync(currentOffsets, null);
                    }
                    count++;
                }
            }
        } finally {
            consumer.close();
        }
    }
}