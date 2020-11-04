package commitMethod;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

/**
 * @Author Natasha
 * @Description
 * @Date 2020/11/3 14:14
 **/

public class ConsumerWithExactlyOnce {

    public static void main(String[] args)  {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "120.27.233.226:9092");
        properties.put("group.id", "test");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("auto.commit.offset", false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singletonList("test"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // 发生分区再均衡之前，提交事务
                commitDBTransaction();
            }
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                // 再均衡之后，从数据库获得消费偏移量
                for(TopicPartition topicPartition : partitions) {
                    consumer.seek(topicPartition, getOffsetFromDB(topicPartition));
                }
            }
        });

        /**
         * 消费之前调用一次 poll()，让消费者加入到消费组中，并获取分配的分区
         * 然后马上调用 seek() 方法定位分区的偏移量
         * seek() 设置消费偏移量，设置的偏移量是从数据库读出来的，说明本次设置的偏移量已经被处理过
         * 下一次调用 poll() 就会在本次设置的偏移量上加1，开始处理没有处理过的数据
         * 如果seek()发生错误，比如偏移量不存在，则会抛出异常
         */
        consumer.poll(Duration.ofMillis(0));
        for(TopicPartition topicPartition : consumer.assignment()) {
            consumer.seek(topicPartition, getOffsetFromDB(topicPartition));
        }

        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
                    // 处理数据
                    processRecord(record);
                    // 把数据存储到数据库中
                    storeRecordInDB(record);
                    // 提交偏移量
                    consumer.commitAsync(currentOffsets, null);
                }
            }
        } finally {
            consumer.close();
        }
    }

    private static void commitDBTransaction() {
    }

    private static void storeRecordInDB(ConsumerRecord<String, String> record) {
    }

    private static void processRecord(ConsumerRecord<String, String> record) {
    }

    private static long getOffsetFromDB(TopicPartition topicPartition) {
        return 0L;
    }
}