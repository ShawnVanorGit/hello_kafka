package commitMethod;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * @Author Natasha
 * @Description
 * @Date 2020/11/3 14:14
 **/

public class ConsumerAsyn {
    public static void main(String[] args)  {
        final Logger log = LoggerFactory.getLogger(ConsumerAsyn.class);

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
                    System.out.println("value = " + record.value() + ", topic = " + record.topic() + ", partition = " + record.partition() + ", offset = " + record.offset());
                }
                // 支持回调函数，用来记录提交错误等
                consumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                        if(exception != null) {
                            log.error("kafka send msg err, exception = {}, offsets = {}", exception, offsets);
                        }
                    }
                });

            }
        }finally {
            consumer.close();
        }
    }
}