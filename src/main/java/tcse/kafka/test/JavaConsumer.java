package tcse.kafka.test;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by DuanSky on 2016/6/24.
 */

public class JavaConsumer {

    private final ConsumerConnector consumer;

    private JavaConsumer() {
        Properties props = new Properties();
        //zookeeper 配置
        props.put("zookeeper.connect", Config.zookeeper);

        //group 代表一个消费组
        props.put("group.id", Config.group);

        //zk连接超时
        props.put("zookeeper.session.timeout.ms", "4000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");
        //序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        ConsumerConfig config = new ConsumerConfig(props);

        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
    }

    void consume() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();

        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());

        for (String topic : Config.topics.split(",")) {
            topicCountMap.put(topic, new Integer(1));
        }

        final Map<String, List<KafkaStream<String, String>>> consumerMap =
                consumer.createMessageStreams(topicCountMap,keyDecoder,valueDecoder);

        for (final String topic : Config.topics.split(",")) {
            new Thread(new Runnable() {
                public void run() {
                    int count = 0;
                    KafkaStream<String, String> stream = consumerMap.get(topic).get(0);
                    ConsumerIterator<String, String> it = stream.iterator();
                    while (it.hasNext()) {
                        count ++;
                        MessageAndMetadata<String,String> message = it.next();
                        System.out.println(count + "#"  + message.topic() +":" + message.key() + ":"+message.message());
                    }
                }
            }).start();

        }

    }

    public static void main(String[] args) {
        new JavaConsumer().consume();
    }
}
