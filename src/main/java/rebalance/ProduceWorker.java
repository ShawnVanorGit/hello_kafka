package rebalance;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.CountDownLatch;

/**
 * @Author Natasha
 * @Description
 * @Date 2020/11/3 14:10
 **/

public class ProduceWorker implements Runnable{

    private ProducerRecord<String,String> record;
    private KafkaProducer<String,String> producer;
    private CountDownLatch countDownLatch;

    public ProduceWorker(ProducerRecord<String, String> record, KafkaProducer<String, String> producer, CountDownLatch countDownLatch) {
        this.record = record;
        this.producer = producer;
        this.countDownLatch = countDownLatch;
    }

    public void run() {
        final String id = "" + Thread.currentThread().getId();
        try {
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(null != exception){
                        exception.printStackTrace();
                    }
                    if(null != metadata){
                        System.out.println(id+"|"+String.format("偏移量：%s,分区：%s", metadata.offset(),metadata.partition()));
                    }
                }
            });
            System.out.println(id+":数据["+record.key()+ "-" + record.value()+"]已发送。");
            countDownLatch.countDown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
