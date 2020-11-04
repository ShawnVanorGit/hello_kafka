package simpledemo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import interceptor.MyConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

/**
 * @Author Natasha
 * @Description
 * @Date 2020/11/3 14:14
 **/

public class ConsumerDemo {

    public static void main(String[] args)  {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "120.27.233.226:9092");
        properties.put("group.id", "test");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, MyConsumerInterceptor.class.getName());


        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

//        List<PartitionInfo> partitionInfos = null;
//        List<TopicPartition> partitions = new ArrayList<>();
//        //主动获取主题下所有的分区。如果你知道所指定的分区，可以跳过这一步
//        partitionInfos = kafkaConsumer.partitionsFor("rebalance-topic-three-part");
//
//        if (partitionInfos != null) {
//            for (PartitionInfo partition : partitionInfos){
//                partitions.add(new TopicPartition(partition.topic(), partition.partition()));
//            }
//            //为消费者指定分区
//            kafkaConsumer.assign(partitions);
//            while (true) {
//                ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
//                for (ConsumerRecord<String, String> record: records) {
//                    System.out.printf("topic = %s, partition = %s, offset = %d, customer = %s, country = %s\n", record.topic(), record.partition(), record.offset(), record.key(), record.value());
//                }
//                kafkaConsumer.commitSync();
//            }
//        }

        kafkaConsumer.subscribe(Collections.singletonList("rebalance-topic-three-part"));
        try{
            //无限循环消处理数据
            while (true) {
                //不断调用poll拉取数据，如果停止拉取，那么Kafka会认为此消费者已经死亡并进行重平衡
                //参数值100是一个超时时间，指明线程如果没有数据会等待多长时间，0表示不等待立即返回
                ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                //每条记录包含key/value以及主题、分区、位移信息
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, value = %s, partition = %s", record.offset(), record.value(), record.partition());
                    System.out.println("=====================>");
                }
            }
        } catch (WakeupException e) {
            // ignore for shutdown
        } finally {
            //此方法会提交位移，同时发送一个退出消费组的消息到Kafka的组协调者，组协调者收到消息后会立即进行重平衡而无需等待此消费者会话过期。
            kafkaConsumer.close();
        }
    }
}