package rebalance;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

/**
 * @Author Natasha
 * @Description
 * @Date 2020/11/3 14:13
 **/

public class ConsumerWorker implements Runnable{

    private final KafkaConsumer<String,String> consumer;
    /*用来保存每个消费者当前读取分区的偏移量*/
    private final Map<TopicPartition, OffsetAndMetadata> currOffsets;
    private final boolean isStop;

    /*消息消费者配置*/
    public ConsumerWorker(boolean isStop, Properties properties) {
        this.isStop = isStop;
        this.consumer = new KafkaConsumer(properties);
        this.currOffsets = new HashMap();
        consumer.subscribe(Arrays.<String>asList("rebalance-topic-three-part"), new HandlerRebalance(currOffsets,consumer));
    }

    public void run() {
        final String id = "" + Thread.currentThread().getId();
        int count = 0;
        TopicPartition topicPartition;
        long offset;
        try {
            while(true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                //业务处理
                //开始事务
                for(ConsumerRecord<String, String> record: records){
                    System.out.println(id+"|"+String.format( "处理主题：%s，分区：%d，偏移量：%d，" + "key：%s，value：%s", record.topic(),record.partition(), record.offset(),record.key(),record.value()));
                    topicPartition = new TopicPartition(record.topic(), record.partition());
                    offset = record.offset()+1;
                    currOffsets.put(topicPartition,new OffsetAndMetadata(offset, "no"));
                    count++;
                    //执行业务sql
                }
                if(currOffsets.size()>0){
                    for(TopicPartition topicPartitionkey:currOffsets.keySet()){
                        HandlerRebalance.partitionOffsetMap.put(topicPartitionkey, currOffsets.get(topicPartitionkey).offset());
                    }
                    //提交事务,同时将业务和偏移量入库
                }
                //如果stop参数为true,这个消费者消费到第5个时自动关闭
                if(isStop&&count>=5){
                    System.out.println(id+"-将关闭，当前偏移量为："+currOffsets);
                    consumer.commitSync();
                    break;
                }
                consumer.commitSync();
            }
        } finally {
            consumer.close();
        }
    }
}

