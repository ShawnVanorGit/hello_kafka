package commitMethod;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @Author Natasha
 * @Description
 * @Date 2020/11/3 14:14
 **/

public class ConsumerCombin {

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
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("value = " + record.value() + ", topic = " + record.topic() + ", partition = " + record.partition() + ", offset = " + record.offset());
                }
                // 如果一切正常，我们使用 commitAsync() 方法来提交
                // 这样速度更快，而且即使这次提交失败，下一次提交很可能会成功
                consumer.commitAsync();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                // 使用 commitSync() 方法会一直重试，直到提交成功或发生无法恢复的错误
                // 确保关闭消费者之前成功提交了偏移量
                consumer.commitSync();
            }finally {
                consumer.close();
            }
        }
    }
}