package interceptor;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author Natasha
 * @Description
 * @Date 2020/11/4 14:32
 **/
public class MyConsumerInterceptor implements ConsumerInterceptor<String,String> {
    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        Map<TopicPartition, List<ConsumerRecord<String, String>>> newRecords = new HashMap<>();
        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<String, String>> recs = records.records(partition);
            List<ConsumerRecord<String, String>> newRecs = new ArrayList<>();
            for (ConsumerRecord<String, String> rec : recs) {
                String newValue = "interceptor-" + rec.value();
                ConsumerRecord<String, String> newRec = new ConsumerRecord<>(rec.topic(), rec.partition(), rec.offset(), rec.key(), newValue);
                newRecs.add(newRec);
            }
            newRecords.put(partition, newRecs);
        }
        return new ConsumerRecords<>(newRecords);
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        offsets.forEach((tp, offsetAndMetadata) -> {
            System.out.println(tp + " : " + offsetAndMetadata.offset());
        });
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
