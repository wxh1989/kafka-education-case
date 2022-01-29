package com.example.kafka;


import com.example.kafka.dao.Student;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import java.util.*;

/**
 * 消费者测试用例
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class KafkaConsumerTest {

    @Autowired
    private Consumer consumer;
    /**
     * 客户端 反序列实体类
     * poll方法 返回消息列表，包含消息主题 分区 分区内偏移量 消息的值
     * 参数是个超时时间，超过指定时间不管有没有值 都返回
     */
    @Test
    public void deserializerByEntity(){
//        Properties props = new Properties();
//        props.put("bootstrap.servers", "192.168.0.99:9092,192.168.0.99:9093,192.168.0.99:9094");
//        props.put("group.id", "WYH-deserializer-app");
//        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        //设置value的反序列化类为自定义序列化类全路径
//        props.put("value.deserializer", MyDeserializer.class.getName());
//
//        KafkaConsumer<String, Student> consumer = new KafkaConsumer<String, Student>(props);
        consumer.subscribe(Collections.singletonList("WYH-serializer-topic"));
        while(true){

            ConsumerRecords<String, Student> records = consumer.poll(100);
            for(ConsumerRecord<String, Student> consumerRecord: records){
                Student student = consumerRecord.value();
                System.out.println(student.toString());
            }
        }

    }




}
