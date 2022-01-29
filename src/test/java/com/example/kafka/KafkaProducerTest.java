package com.example.kafka;

import com.example.kafka.avro.User;
import com.example.kafka.callback.ProducerCallback;
import com.example.kafka.dao.Customer;
import com.example.kafka.dao.Student;
import com.example.kafka.serializer.AvroSerializer;
import com.example.kafka.serializer.CustomerSerializer;
import com.example.kafka.serializer.MySerializer;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.internals.Topic;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 生产者测试用例
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class KafkaProducerTest {

    @Autowired
    Producer producer;
    @Autowired
    AdminClient adminClient;

    /**
     * 基础功能
     */
    @Test
    public void sendMessage(){
        ProducerRecord<String,String> record = new ProducerRecord<>("SpringClient","CreateMessage","wxh");
        producer.send(record);
    }
    /**
     * 同步发送消息
     * 消息发送到 kafka broker 上等到有返回值才能继续操作
     */
    @Test
    public void sendSynchroMessage(){
        ProducerRecord<String,String> record = new ProducerRecord<>("SynchroMessage","CreateMessage","这是一条同步消息");
        try {
            System.out.println(producer.send(record).get());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    /**
     * 异步消息
     */
    @Test
    public void sendNoSynchroMessage(){

        ProducerRecord<String,String> record = new ProducerRecord<String,String>("SynchroMessage","CreateMessage","这是一要异步消息");
        // 第二个参数创建 一个回调函数，返回值会自动 在回调函数内体现
        producer.send(record,new ProducerCallback());
    }

    /**
     * 使用自定义序列化发送实体类
     */
    @Test
    public void  sendDaoMessage(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.0.99:9092,192.168.0.99:9093,192.168.0.99:9094");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //设置value的序列化类为自定义序列化类全路径
        props.put("value.serializer", MySerializer.class.getName());
        Producer<String, Student> producer = new KafkaProducer<String, Student>(props);
        //定义一个要发送的Student类型的value
        Student s = new Student("wxh1", 14);
        Student s2 = new Student("ln2", 17);
        //参数一为topic,参数二为value
        ProducerRecord<String, Student> record = new ProducerRecord<String, Student>("WYH-serializer-topic", s);
        ProducerRecord<String, Student> record1 = new ProducerRecord<String, Student>("WYH-serializer-topic", s2);
        producer.send(record);
        producer.send(record1);
        producer.close();

    }
    /**
     * 创建分区
     */
    @Test
    public void createTopic(String topic ,int partition,short replication){
        //创建分区 4个 分区 1个副本
        NewTopic newTopic = new NewTopic(topic,partition,replication);
        List<NewTopic> topics = new ArrayList<>();
        topics.add(newTopic);
        adminClient.createTopics(topics);
        adminClient.close();
    }
    /**
     * 使用自定义分区器
     */
    @Test
    public void  sendCustomPartitioner(){
        String topic = "CustomPartitioner";
        this.createTopic(topic,4, (short) 4);
        ProducerRecord<String,String> record = new ProducerRecord<>(topic,"maxOrder","发送到最后一个分区New1");
        RecordMetadata metadata = null;
        try {
            metadata = (RecordMetadata) producer.send(record).get();
            System.out.println(metadata.partition());

            record = new ProducerRecord<>(topic,"other","其他分区信息消息1");
            metadata = (RecordMetadata) producer.send(record).get();
            System.out.println(metadata.partition());

            record = new ProducerRecord<>(topic,"other","其他分区消息21");
            metadata = (RecordMetadata) producer.send(record).get();
            System.out.println(metadata.partition());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

}
