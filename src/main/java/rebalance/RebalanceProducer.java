package rebalance;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @Author Natasha
 * @Description
 * @Date 2020/11/3 14:07
 **/
public class RebalanceProducer {
    private static final int MSG_SIZE = 50;
    private static ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private static CountDownLatch countDownLatch = new CountDownLatch(MSG_SIZE);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"120.27.233.226:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaProducer<String,String> producer = new KafkaProducer(properties);
        try {
            for(int i=0;i<MSG_SIZE;i++){
                ProducerRecord<String,String> record = new ProducerRecord("rebalance-topic-three-part","value" + i);
                executorService.submit(new ProduceWorker(record,producer,countDownLatch));
                Thread.sleep(600);
            }
            countDownLatch.await();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
            executorService.shutdown();
        }
    }
}
