package com.lhs.flink.example.java.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @ClassNameMyKafkaProducer
 * @Description
 * @Author lihuasong
 * @Date2020/4/19 10:51
 * @Version V1.0
 **/
public class MyKafkaProducer {

    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();

        properties.put("bootstrap.servers","localhost:9092");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer producer = new KafkaProducer(properties);

        while (true){
            long current = System.currentTimeMillis();
            String str = "{\"key\":\"key\",\"field\":\"field\",\"value\":\"value\",\"time\":\""+current+"\"}";
            ProducerRecord<String,String> record1 = new ProducerRecord<>("flink",str);
            producer.send(record1);
            System.out.println("k1,"+current);
            Thread.sleep(1000L);
        }

    }

}
