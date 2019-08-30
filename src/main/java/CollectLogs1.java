
    import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Properties;

public class CollectLogs1 {
        public static void main(String[] args) {
            Properties properties = new Properties();
            properties.setProperty("metadata.broker.list", "192.168.91.7:9092,192.168.91.8:9092,192.168.91.9:9092");
            //消息传递到broker时的序列化方式
            properties.setProperty("serializer.class", StringEncoder.class.getName());
            //zk的地址
            properties.setProperty("zookeeper.connect", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
            //是否反馈消息 0是不反馈消息 1是反馈消息
            properties.setProperty("request.required.acks", "1");
            ProducerConfig producerConfig = new ProducerConfig(properties);
            Producer<String, String> producer = new Producer(producerConfig);
            try {
                BufferedReader bf = new BufferedReader(new FileReader(new File("D://qianfeng/spark/cmcc.json")));
                String line;
                while ((line = bf.readLine()) != null) {
                    KeyedMessage<String, String> keyedMessage = new KeyedMessage("lg", line);
                    Thread.sleep(500);
                    producer.send(keyedMessage);
                    System.out.println(keyedMessage);
                }
                bf.close();
                producer.close();
                System.out.println("发送完毕");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


