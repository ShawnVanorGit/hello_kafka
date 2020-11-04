### 消费者和消费者群组

**1. 一个消费者**从一个Topic中消费数据 ：

![img](//p3-juejin.byteimg.com/tos-cn-i-k3u1fbpfcp/c97a6361311f411f971b917391d8bb2c~tplv-k3u1fbpfcp-zoom-1.image)

**2. 消费者群组 :**

当生产者向 Topic 写入消息的速度超过了现有消费者的处理速度，此时需要对消费者进行横向伸缩，用多个消费者从同一个主题读取消息，对消息进行分流。**同一个分区不能被一个组中的多个 consumer 消费。**

![two](//p3-juejin.byteimg.com/tos-cn-i-k3u1fbpfcp/ee85ef484b384cb985d8b43954f4f21c~tplv-k3u1fbpfcp-zoom-1.image)
### Kafka消费者代码样例

读取Kafka消息只需要创建一个KafkaConsumer，除此之外还需要使用四个基本属性，bootstrap.servers、key.deserializer、value.deserializer和group.id。

```java
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/**
 * @Author Natasha
 * @Description
 * @Date 2020/11/3 14:14
 **/

public class ConsumerDemo {
    public static void main(String[] args)  {
        Properties properties = new Properties();
        //bootstrap.servers是broker服务器列表
        properties.put("bootstrap.servers", "120.27.233.226:9092");
        //group.id是指是消费者的消费组
        properties.put("group.id", "test");
        //key.deserializer和value.deserializer是用来做反序列化的，也就是将字节数组转换成对象
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("session.timeout.ms", "30000");
        
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        //订阅主题
        //kafkaConsumer.subscribe(Collections.singletonList("test"));
        kafkaConsumer.assign(Arrays.asList(new TopicPartition("test",0)));
        try{
            //无限循环消处理数据
            while (true) {
                //不断调用poll拉取数据，如果停止拉取，那么Kafka会认为此消费者已经死亡并进行重平衡
                //参数值100是一个超时时间，指明线程如果没有数据会等待多长时间，0表示不等待立即返回
                ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                //每条记录包含key/value以及主题、分区、位移信息
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, value = %s\n", record.offset(), record.value());
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
```

### 提交偏移量

#### 1. 自动提交

最简单的提交方式是让消费者自动提交偏移量，如果 enable.auto.commit 被设为 true，那么每过 5s，消费者会自动把从 poll() 方法接收到的最大偏移量提交上去。

可能造成的问题：数据重复读

假设我们仍然使用默认的 5s 提交时间间隔，在最近一次提交之后的 3s 发生了再均衡，再均衡之后，消费者从最后一次提交的偏移量位置开始读取消息。这个时候偏移量已经落后了 3s，所以在这 3s内到达的消息会被重复处理。可以通过修改提交时间间隔来更频繁地提交偏移量，减小可能出现重复消息的时间窗，不过这种情况是无法完全避免的。

```Java
properties.put("enable.auto.commit", "true");
```

#### 2. 手动提交

**2.1 同步提交**

```java
public static void main(String[] args)  {
    Properties properties = new Properties();
    properties.put("bootstrap.servers", "120.27.233.226:9092");
    properties.put("group.id", "test");
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    // 把auto.commit.offset设为false，让应用程序决定何时提交偏移量
    properties.put("auto.commit.offset", false);

    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
    consumer.subscribe(Collections.singletonList("test"));
    try{
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for(ConsumerRecord<String, String> record : records) {
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
```

**2.2 异步提交**

手动提交有一个不足之处，在 broker 对提交请求作出回应之前，应用程序会一直阻塞，这样会限制应用程序的吞吐量。我们可以通过降低提交频率来提升吞吐量，但如果发生了再均衡，会增加重复消息的数量。

这时可以使用异步提交，只管发送提交请求，无需等待 broker 的响应。它之所以不进行重试，是因为在它收到服务器响应的时候，可能有一个更大的偏移量已经提交成功。

假设我们发出一个请求用于提交偏移量2000，这个时候发生了短暂的通信问题，服务器收不到请求，自然也不会作出任何响应。与此同时，我们处理了另外一批消息，并成功提交了偏移量3000。如果commitAsync()重新尝试提交偏移量2000，它有可能在偏移量3000之后提交成功。这个时候如果发生再均衡，就会出现重复消息。

```Java
try{
    while(true) {
        ConsumerRecords<String, String> records = consumer.poll(1000);
        for(ConsumerRecord<String, String> record : records) {
            System.out.println("value = " + record.value() + ", topic = " + record.topic() + ", partition = " + record.partition() + ", offset = " + record.offset());
        }
        // 提交最后一个偏移量，然后继续做其他事情。
        consumer.commitAsync();
    }
}finally {
    consumer.close();
}
```

commitAsync()也支持回调，在broker作出响应时会执行回调，回调经常被用于记录提交错误或生成度量指标；如果要用它来进行重试，一定要注意提交的顺序。

```jade
try{
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
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
```

> 可以在回调中重试失败的提交的思路： 
>
> 使用一个单调递增的序列号来维护异步提交的顺序。在每次提交偏移量之后或在回调里提交偏移量时递增序列号。在进行重试前，先检查回调的序列号和即将提交的偏移量是否相等，如果相等，说明没有新的提交，那么可以安全地进行重试。如果序列号比较大，说明有一个新的提交已经发送出去了，应该停止重试。

**2.3 混合同步提交与异步提交**

一般情况下，针对偶尔出现的提交失败，不进行重试不会有太大问题，因为如果提交失败是因为临时问题导致的，那么后续的提交总会有成功的。但如果这是发生在关闭消费者或再均衡前的最后一次提交，就要确保能够提交成功。

```java
try {
    while (true) {
        ConsumerRecords<String, String> records = consumer.poll(1000);
        for (ConsumerRecord<String, String> record : records) {
            System.out.println("value = " + record.value() + ", topic = " + record.topic() + ", partition = " + record.partition() + ", offset = " + record.offset());
        }
        // 如果一切正常，我们使用 commitAsync() 方法来提交
        // 这样速度更快，而且即使这次提交失败，下一次提交很可能会成功
        consumer.commitAsync();
    }
} catch (Exception e) {
    e.printStackTrace();
} finally {
    try {
        // 使用 commitSync() 方法会一直重试，直到提交成功或发生无法恢复的错误
        // 确保关闭消费者之前成功提交了偏移量
        consumer.commitSync();
    }finally {
        consumer.close();
    }
}
```

### 从特定偏移量开始处理记录

不管是自动提交还是使用commitAsync()或者commitSync()来提交偏移量，提交的都是 poll() 方法返回的那批数据的最大偏移量。KafkaConsumer API 允许在调用 `commitSync()` 和 `commitAsync()` 方法允许我们指定特定的位移参数，参数为提交的分区与偏移量的map。因为消费者可能不只读取一个分区，需要跟踪所有分区的偏移量，所以在这个层面上控制偏移量的提交会让代码变复杂。

```
while (true) {
    ConsumerRecords<String, String> records = consumer.poll(1000);
    for (ConsumerRecord<String, String> record : records) {
        System.out.println("value = " + record.value() + ", topic = " + record.topic() + ", partition = " + record.partition() + ", offset = " + record.offset());
        currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1, "no metadata"));
        if (count % 1000 == 0) {
            // 这里调用的是 commitAsync()，不过调用 commitSync() 也是完全可以的
            // 当然，在提交特定偏移量时，仍然要处理可能发生的错误
            consumer.commitAsync(currentOffsets, null);
        }
        count++;
    }
}
```

### 数据库的 Exactly Once 语义的实现思路

当处理 Kafka 中的数据涉及到数据库时：假设把数据存储到数据库后，如果没有来得及提交偏移量程序就因某种原因挂掉了，那么程序再次启动后就会重复处理数据，数据库中会有重复的数据。

如果把存储到数据库和提交偏移量在一个原子操作里完成，就可以避免这样的问题，但数据存到数据库，偏移量保存到kafka是无法实现原子操作的，而如果把数据存储到数据库中，偏移量也存储到数据库中，这样就可以利用数据库的事务来把这两个操作设为一个原子操作，同时结合再均衡监听器就可以实现 Exactly Once 语义，以下为伪代码：

```
KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
consumer.subscribe(Collections<String> topics, new ConsumerRebalanceListener() {
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
consumer.poll(0);
for(TopicPartition topicPartition : consumer.assignment()) {
    consumer.seek(topicPartition, getOffsetFromDB(topicPartition));
}

Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
try {
    while (true) {
        ConsumerRecords<String, String> records = consumer.poll(100);
        for (ConsumerRecord<String, String> record : records) {
            currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1);
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
```

**把偏移量和记录保存到用一个外部系统来实现 Exactly Once 有很多方法，但核心思想都是：结合 ConsumerRebalanceListener 和 seek() 方法来确保能够及时保存偏移量，并保证消费者总是能够从正确的位置开始读取消息。**

### 再均衡

分区的所有权从一个消费者转移到另一个消费者，这样的行为被称为再均衡（Rebalance）。再均衡非常重要，为消费者组带来了高可用性和伸缩性，可以放心的增加或移除消费者。以下是触发再均衡的三种行为：

1. 当一个 消费者 加入组时，读取了原本由其他消费者读取的分区，会触发再均衡。
2. 当一个 消费者 离开组时（被关闭或发生崩溃），原本由它读取的分区将被组里的其他 消费者 来读取，会触发再均衡。
3. 当 Topic 发生变化时，比如添加了新的分区，会发生分区重分配，会触发再均衡。

消费者通过向作为组协调器的 broker发送心跳来维持**和群组以及分区**的关系。心跳的意思是表明消费者在读取分区里的消息。消费者会在轮询消息或提交偏移量时发送心跳。如果消费者超过一定时间没有发送心跳，会话就会过期，组协调器认为该消费者宕机，会触发再均衡。可以看到，从消费者会话过期到宕机是有一定时间的，这段时间内该消费者的分区都不能进行消息消费。

> 在 Kafka 0.10.1版本，Kafka对心跳机制进行了修改，将发送心跳与拉取消息进行分离，这样使得发送心跳的频率不受拉取的频率影响

### 再均衡监听器

在分区重平衡前，如果消费者知道它即将不再负责某个分区，那么它需要将它已经处理过的消息位移提交。Kafka的API允许我们在消费者新增分区或者失去分区时进行处理，我们只需要在调用subscribe()方法时传入ConsumerRebalanceListener对象，该对象有两个方法：

```
public void onPartitionRevoked(Collection partitions)：此方法会在消费者停止消费后，在重平衡开始前调用。
public void onPartitionAssigned(Collection partitions)：此方法在分区分配给消费者后，在消费者开始读取消息前调用。
```

相关的实战项目具体代码可以看 [(四)Kafka 再均衡监听器 实战小例子](https://juejin.im/post/6890806474835951623) ，可以把代码拷下来在本地运行。

### 序列化和反序列化

Kafka 生产者将对象序列化成字节数组并发送到服务器，消费者需要将字节数组转换成对象（反序列化）。序列化与反序列化需要匹配，与生产者类似，推荐使用 Avro 序列化方式。（关于生产者的序列化可以参考 [(三)Kafka的生产者原理及使用详解](https://juejin.im/post/6890400602657030152) ）

```csharp
Properties props = new Properties();
props.put("bootstrap.servers", "120.27.233.226:9092");
props.put("group.id", "test");
props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
props.put("schema.registry.url", schemaUrl);
String topic = "test"

KafkaConsumer consumer = new KafkaConsumer(createConsumerConfig(brokers, groupId, url));
consumer.subscribe(Collections.singletonList(topic));

while (true) {
    // 这里使用之前生产者使用的Avro生成的Customer类
    ConsumerRecords<String, Customer> records = consumer.poll(1000);
    for (ConsumerRecord<String, Customer> record: records) {
        System.out.println("Current customer name is: " + record.value().getName());
    }
    consumer.commitSync();
}
```

### Kafka的分区分配过程

1. 确定`群组协调器（GroupCoordinator）`，每当我们创建一个消费组，kafka 会为我们分配一个 broker 作为该消费组的 coordinator。
2. 注册消费者 并选出 `leader consumer`，当我们的有了 coordinator 之后，消费者将会开始往该 coordinator上进行注册，第一个注册的消费者将成为该消费组的 leader，其他的为 follower.
3. 当 leader 选出来后，他会从coordinator那里实时获取分区 和 consumer信息，并根据分区策略给每个consumer分配分区，并将分配结果告诉 coordinator。
4. follower 消费者将从 coordinator 那里获取到自己相关的分区信息进行消费，对于所有的 follower 消费者而言， 他们只知道自己消费的分区，并不知道其他消费者的存在。
5. 至此，消费者都知道自己的消费的分区，分区过程结束，当发生 分区再均衡 的时候，leader 将会重复分配过程。

### 消费者分区分配策略

#### 1. Range

将partitions的个数除于消费者线程的总数来决定每个消费者线程消费几个分区。如果除不尽，那么前面几个消费者线程将会多消费一个分区。

> 我们有10个分区，3个消费者线程， 10 / 3 = 3，而且除不尽，那么消费者线程 C1-0 将会多消费一个分区：
> - C1-0 将消费 0, 1, 2, 3 分区
> - C2-0 将消费 4, 5, 6 分区
> - C2-1 将消费 7, 8, 9 分区

#### 2. RoundRobin

RoundRobin策略的工作原理：将所有主题的分区组成 TopicAndPartition 列表，然后对 TopicAndPartition 列表按照 hashCode 进行排序，然后通过轮询方式逐个将分区以此分配给每个消费者。

> 如按照 hashCode 排序完的topic-partitions组依次为T1-5, T1-3, T1-0, T1-8, T1-2, T1-1, T1-4, T1-7, T1-6, T1-9，我们的消费者线程排序为C1-0, C1-1, C2-0, C2-1，最后分区分配的结果为：
>
> - C1-0 将消费 T1-5, T1-2, T1-6 分区；
> - C1-1 将消费 T1-3, T1-1, T1-9 分区；
> - C2-0 将消费 T1-0, T1-4 分区；
> - C2-1 将消费 T1-8, T1-7 分区；

### 消费者拦截器

消费者拦截器主要在消费到消息或在提交消费位移时进行一些定制化的操作，只需要实现ConsumerInterceptor类中的方法就可以：

```text
 public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records);
 public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets);
 public void close();
```

它会在poll方法返回之前调用拦截器的onConsume（）方法来对消息进行相应的定制化操作，比如修改返回的消息内容，按照某些规则进行过滤数据。

它会在提交完消费位移之后调用拦截器的onCommit（）方法，可以使用这个方法来记录跟踪所提交的位移信息。

```
properties.setProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, MyConsumerInterceptor.class.getName());
```

```java
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
```

### 独立的消费者

一般情况下我们都是使用消费组（即便只有一个消费者）来消费消息的，因为这样可以在增加或减少消费者时自动进行分区重平衡。这种方式是推荐的方式。

如果在知道主题和分区的情况下，我们也可以使用单个消费者来进行消费。对于这种情况，我们需要的是给消费者分配消费分区，而不是让消费者订阅（成为消费组）主题。

```
List<PartitionInfo> partitionInfos = null;
List<TopicPartition> partitions = new ArrayList<>();
//主动获取主题下所有的分区。如果你知道所指定的分区，可以跳过这一步
partitionInfos = kafkaConsumer.partitionsFor("rebalance-topic-three-part");

if (partitionInfos != null) {
    for (PartitionInfo partition : partitionInfos){
    	partitions.add(new TopicPartition(partition.topic(), partition.partition()));
    }
    //为消费者指定分区
    kafkaConsumer.assign(partitions);
    while (true) {
    	ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
    	for (ConsumerRecord<String, String> record: records) {
    		System.out.printf("topic = %s, partition = %s, offset = %d, customer = %s, country = %s\n", record.topic(), record.partition(), record.offset(), record.key(), record.value());
    	}
    	kafkaConsumer.commitSync();
    }
}
```

除了需要主动获取分区以及没有分区重平衡，其他的处理逻辑都是一样的。需要注意的是，如果添加了新的分区，这个消费者是感知不到的，需要通过consumer.partitionsFor()来重新获取分区。

### 消费者配置

Kafka 与消费者相关的配置大部分参数都有合理的默认值，一般不需要修改，不过有一些参数与消费者的性能和可用性有很大关系。接下来介绍这些重要的属性。

1. `fetch.min.bytes`

指定消费者从服务器获取记录的最小字节数。如果服务器在收到消费者的数据小于 `fetch.min.bytes`，那么会等到有足够的可用数据时才返回给消费者。

合理的设置可以降低消费者和 broker 的工作负载，在 Topic 消息生产不活跃时，减少处理消息次数。

> 如果没有很多可用数据，但消费者的 CPU 使用率却很高，需要调高该属性的值。
>
> 如果消费者的数量比较多，调高该属性的值也可以降低 broker 的工作负载。

2. `fetch.max.wait.ms`

指定在 broker 中的等待时间，默认是500ms。如果没有足够的数据流入 Kafka，即数据量没有达到 `fetch.min.bytes`，500ms后会返回数据给消费者。

`fetch.max.wait.ms` 和 `fetch.min.bytes` 有一个满足条件就会返回数据。

3. `max.parition.fetch.bytes`

指定了服务器从每个分区里的数据返回给消费者的最大字节数，默认值是1MB。

如果一个主题有20个分区和5个消费者（同一个组内），那么每个消费者需要至少4MB 的可用内存（每个消费者读取4个分区）来接收记录。如果组内有消费者发生崩溃，剩下的消费者需要处理更多的分区。

`max.parition.fetch.bytes` 必须比 broker 能够接收的最大消息的字节数（`max.message.size`）大，否则消费者可能无法读取这些消息，导致消费者一直重试。

> 另一个需要考虑的因素是消费者处理数据的时间。消费者需要频繁调用 `poll()` 方法来避免会话过期和发生分区再均衡，如果单次调用 `poll()` 返回的数据太多，消费者需要更多的时间来处理，可能无法及时进行下一个轮询来避免会话过期。如果出现这种情况，可以把 `max.parition.fetch.bytes` 值改小或者延长会话过期时间。

4. `session.timeout.ms`

指定了消费者与服务器断开连接的最大时间，默认是3s。如果消费者没有在指定的时间内发送心跳给 GroupCoordinator，就被认为已经死亡，会触发再均衡，把它的分区分配给其他消费者。

> 该属性与 `heartbeat.interval.ms` 紧密相关，`heartbeat.interval.ms` 指定了 `poll()` 方法向协调器发送心跳的频率，`session.timeout.ms` 指定了消费者最长多久不发送心跳。所以，一般需要同时修改这两个属性，`heartbeat.interval.ms` 必须比 `session.timeout.ms` 小，一般是 `session.timeout.ms` 的三分之一，如果 `session.timeout.ms` 是 3s，那么 `heartbeat.interval.ms` 应该是 1s。

5. `auto.offset.reset`

指定了消费者在读取一个没有偏移量的分区或者偏移量无效的情况下默认是 `latest`，另一个值是 `earliest`，消费者将从起始位置读取分区的记录。

6. `enable.auto.commit`

指定了消费者是否自动提交偏移量，默认值是 `true`，自动提交。如果设为 `true`，需要通过配置 `auto.commit.interval.ms` 属性来控制提交的频率。设为 `false` 可以程序自己控制何时提交偏移量。

7. `partition.assignment.strategy`

决定哪些分区应该被分配给哪个消费者。Kafka 有两个默认的分配策略：

- Range，把 Topic 的若干个连续的分区分配给消费者。
- RoundRobin，把所有分区逐个分配给消费者。

默认值是 `org.apache.kafka.clients.consumer.RangeAssignor`，这个类实现了 `Range` 策略。

8. `receive.buffer.bytes` 和 `send.buffer.bytes`

分别指定了 TCP socket 接收和发送数据包的缓冲区大小。如果设为-1就使用操作系统的默认值。如果生产者或消费者与 broker 处于不同的数据中心，那么可以适当增大这些值，因为跨数据中心的网络一般都有比较高的延迟和比较低的带宽。

### 优雅退出

```Java
package graceexit;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

/**
 * @Author Natasha
 * @Description
 * @Date 2020/11/4 14:21
 **/
public class QuitConsumer {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "120.27.233.226:9092");
        props.put("group.id", "test");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList("test"));
        final Thread mainThread = Thread.currentThread();

        /*
         * 注册 JVM 关闭时的回调，当 JVM 关闭时调用
         * 退出循环需要通过另一个线程调用consumer.wakeup()方法
         * 调用consumer.wakeup()可以退出poll(),并抛出WakeupException异常
         * 我们不需要处理 WakeupException,因为它只是用于跳出循环的一种方式
         * consumer.wakeup()是消费者唯一一个可以从其他线程里安全调用的方法
         */
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Starting exit...");
                // 调用消费者的 wakeup 方法通知主线程退出
                consumer.wakeup();
                try {
                    // 主线程继续执行，以便可以关闭consumer，提交偏移量
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("topic = " + record.topic() + ", partition = " + record.partition() + ", offset = " + record.offset());
                }
                consumer.commitAsync();
            }
        }catch (WakeupException e) {
            // 不处理异常
        } finally {
            // 在退出线程之前调用consumer.close()是很有必要的，它会提交任何还没有提交的东西，并向组协调器发送消息，告知自己要离开群组。
            // 接下来就会触发再均衡，而不需要等待会话超时。
            consumer.commitSync();
            consumer.close();
            System.out.println("Closed consumer and we are done");
        }
    }
}
```

### github 

本文章中相关代码样例已上传 github ：[https://github.com/ShawnVanorGit/hello_kafka](https://github.com/ShawnVanorGit/hello_kafka)

![](https://p1-juejin.byteimg.com/tos-cn-i-k3u1fbpfcp/15c0a684f02149559179284d5ebc050b~tplv-k3u1fbpfcp-watermark.image)
