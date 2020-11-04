package commitMethod;

import org.apache.kafka.clients.consumer.CommitFailedException;
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

public class ConsumerSync {

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
        try{
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for(ConsumerRecord<String, String> record : records) {
                    // 假设把记录内容打印出来就算处理完毕
                    System.out.println("value = " + record.value() + ", topic = " + record.topic() + ", partition = " + record.partition() + ", offset = " + record.offset());
                }
                try{
                    // 只要没有发生不可恢复的错误，commitSync() 方法会一直尝试直至提交成功
                    consumer.commitSync();
                }catch(CommitFailedException e) {
                    // 如果提交失败，我们也只能把异常记录到错误日志里
                    System.err.println("commit  failed!" + e.getMessage());
                }
            }
        }finally {
            consumer.close();
        }
    }
}