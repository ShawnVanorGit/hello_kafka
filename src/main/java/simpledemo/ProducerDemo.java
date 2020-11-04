package simpledemo;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerDemo {

    public static void main(String[] args){
        //1. 设置参数
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "120.27.233.226:9092");

        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        properties.put("acks", "-1");
        properties.put("retries", 3);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 10);
        properties.put("buffer.memory", 33554432);
        properties.put("max.block.ms", 3000);
        properties.put("max.request.size", 1048576);
        properties.put("request.timeout.ms", 30000);

        //2.创建Producer实例，跟broker建立socket
        Producer<String, String> producer = null;
        try {
            producer = new KafkaProducer<String, String>(properties);
            for (int i = 0; i < 100; i++) {
                String msg = "This is Message " + i;
                //3. 创建消息
                ProducerRecord<String, String> record = new ProducerRecord("rebalance-topic-three-part",  msg);
                //4. 发送消息
                producer.send(record);
                System.out.println("Sent:" + msg );

                Thread.sleep(3000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //5. 关闭连接
            producer.close();
        }

    }
}