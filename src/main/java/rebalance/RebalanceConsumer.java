package rebalance;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
/**
 * @Author Natasha
 * @Description
 * @Date 2020/11/3 14:37
 **/

public class RebalanceConsumer {

    public static final String GROUP_ID = "RebalanceConsumer";
    private static ExecutorService executorService = Executors.newFixedThreadPool(3);


    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"120.27.233.226:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, RebalanceConsumer.GROUP_ID);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        for(int i = 0; i < 2; i++){
            executorService.submit(new ConsumerWorker(false, properties));
        }
        Thread.sleep(1000);
        //用来被停止，观察保持运行的消费者情况
        new Thread(new ConsumerWorker(true, properties)).start();
    }
}
