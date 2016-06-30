package tcse.join.test;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import tcse.kafka.test.Config;

import java.io.PrintWriter;
import java.util.*;

/**
 * Created by DuanSky on 2016/6/27.
 */
public class JavaJoinProducer {

    private final Producer<String, String> producer;
    private final int MAX = Integer.MAX_VALUE;
    private final Random random = new Random();

    public static void main(String args[]){
        new JavaJoinProducer().produce();
    }


    private void writeResultToFile(String topic,List<String> keys, String path){
        try {
            PrintWriter writer = new PrintWriter(path);
            for(String key : keys){
                String value = topic + ":" + new Date().toString();
                writer.println("("+key+","+value+")");
            }
            writer.flush();
            writer.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public JavaJoinProducer(){

        Properties props = new Properties();
        //此处配置的是kafka的端口
        props.put("metadata.broker.list", Config.brokers);

        //配置value的序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //配置key的序列化类
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");

        //request.required.acks
        //0, which means that the producer never waits for an acknowledgement from the broker (the same behavior as 0.7). This option provides the lowest latency but the weakest durability guarantees (some data will be lost when a server fails).
        //1, which means that the producer gets an acknowledgement after the leader replica has received the data. This option provides better durability as the client waits until the server acknowledges the request as successful (only messages that were written to the now-dead leader but not yet replicated will be lost).
        //-1, which means that the producer gets an acknowledgement after all in-sync replicas have received the data. This option provides the best durability, we guarantee that no messages will be lost as long as at least one in sync replica remains.
        props.put("request.required.acks","-1");

        producer = new Producer<String, String>(new ProducerConfig(props));
    }

    void produce() {
        List<String> aKeys = new LinkedList<String>(), bKeys = new LinkedList<String>();

        Set<String> crossSet = new HashSet<String>(), aKeySet = new HashSet<String>(), bKeySet = new HashSet<String>();
        // the cross keys
        while(crossSet.size() != JoinConfig.crossNum())
            crossSet.add(random.nextInt(MAX)+"");
        while(aKeySet.size() != JoinConfig.aNum() - JoinConfig.crossNum()){
            String curr = random.nextInt(MAX)+"";
            if(!crossSet.contains(curr))
                aKeySet.add(curr);
        }
        while(bKeySet.size() != JoinConfig.bNum() - JoinConfig.crossNum()){
            String curr = random.nextInt(MAX)+"";
            if(!crossSet.contains(curr) && !aKeySet.contains(curr))
                bKeySet.add(curr);
        }
        aKeys.addAll(aKeySet); aKeys.addAll(crossSet);
        bKeys.addAll(bKeySet); bKeys.addAll(crossSet);

        String[] topics = JoinConfig.topics().split(",");

        //write data to files.
        writeResultToFile(topics[0],aKeys,JoinConfig.aFilePath());
        writeResultToFile(topics[1],bKeys,JoinConfig.bFilePath());

        int aPos = 0, bPos = 0;

        while(aPos != aKeys.size() && bPos != bKeys.size()){
            if(aPos != aKeys.size()){
                String key = aKeys.get(aPos);
                String data = topics[0] + "-" + new Date();
                producer.send(new KeyedMessage<String, String>(topics[0], key ,data));
                System.out.println(topics[0] + "#" + (aPos+1) + "#" + data);
                aPos++;
            }
            if(bPos != bKeys.size()){
                String key = bKeys.get(bPos);
                String data = topics[1] + "-" + new Date();
                producer.send(new KeyedMessage<String, String>(topics[1], key ,data));
                System.out.println(topics[1] + "#" + (bPos+1) + "#" + data);
                bPos++;
            }
        }
    }
}
