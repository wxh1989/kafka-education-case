package com.example.kafka;

import com.example.kafka.dao.KafkaOffset;
import com.example.kafka.dao.Student;
import com.example.kafka.mapper.KafkaOffsetMapper;
import com.example.kafka.serializer.MyDeserializer;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * 消费者重复消费消息的场景
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class KafkaRepeatOffset {


    @Resource
    private KafkaOffsetMapper kafkaOffsetMapper;
    @Autowired
    private AdminClient adminClient;
    @Autowired
    private Producer producer;

    private boolean flag = true;

    @Test
    /**
     * 创建主题
     */
    public void createRepeatTopic(){
        //创建分区 4个分区 4个副本
        NewTopic newTopic = new NewTopic("repeat-topic",2, (short) 1);
        List<NewTopic> topics = new ArrayList<>();
        topics.add(newTopic);
        CreateTopicsResult createTopicsResult = adminClient.createTopics(topics);
        try {
            createTopicsResult.all().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        adminClient.close();
    }
    /**
     * 修改分区
     */
    @Test
    public void edit(){
        Map newPartitions = new HashMap();
        //创建新的分区的结果
        newPartitions.put("repeat-topic", NewPartitions.increaseTo(2));
        CreatePartitionsResult createPartitionsResult = adminClient.createPartitions(newPartitions);
        try {
            createPartitionsResult.all().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
    /**
     * 生产者创建消息
     */
    @Test
    public void sendMessage(){
        int offset = kafkaOffsetMapper.selectMaxOffset();
        for(int i = 0 ;i < offset + 10; i++){
            ProducerRecord<String,String> record =
                    new ProducerRecord<>("repeat-topic",
                            "key"+i,
                            "这是第"+i+"条消息");
            try {
                Thread.sleep(1000);
                System.out.println(producer.send(record).get().toString());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 消费者一
     *
     */
    @Test
    public void consumerGroupOne(){
        Properties group_one =  new Properties();
        group_one.put("key.deserializer", StringDeserializer.class.getName());
        group_one.put("value.deserializer", StringDeserializer.class.getName());
        group_one.put("bootstrap.servers","192.168.0.99:9092,192.168.0.99:9093,192.168.0.99:9094");
        group_one.put("group.id","repeat-group");
        //关闭自动提交偏移量
        group_one.put("enable.auto.commit",false);

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(group_one);
        //订阅消息
        consumer.subscribe(Collections.singletonList("repeat-topic"));
        KafkaOffset kafkaOffset = null;
        int count = 0;
        try {
            while (true){
                ConsumerRecords<String, String> records = consumer.poll(100);
                for(ConsumerRecord<String, String> consumerRecord: records){
                    kafkaOffset = new KafkaOffset();
                    kafkaOffset.setConsumer("consumer-1");
                    kafkaOffset.setTopic(consumerRecord.topic());
                    kafkaOffset.setTopicPartition(consumerRecord.partition());
                    kafkaOffset.setKey(consumerRecord.key());
                    kafkaOffset.setMessageValue(consumerRecord.value());
                    kafkaOffset.setPartitionOffset((int) consumerRecord.offset());
                    kafkaOffsetMapper.insert(kafkaOffset);
                    count++;
                }
                //还没来得及提交偏移量 消费者 挂掉了，然后重新上线 消费的数据将会重复消费
                if(records.count() > 0){
                    //或者网络波动 一直没提交成功
                    //或者发生 分区在均衡 都会导致 消息重复
                    System.out.println("没来得及提交偏移量,系统挂掉了");
                    System.exit(0);
                    consumer.commitSync();
                }

            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            consumer.close();
        }

    }
    /**
     * 消费者2
     */
    @Test
    public void consumerGroupTwo() throws InterruptedException {
        //5秒后 消费者加入 消费者组 分区将重新分配  触发在均衡
//        Thread.sleep(5000);
        Properties group_one =  new Properties();
        group_one.put("key.deserializer", StringDeserializer.class.getName());
        group_one.put("value.deserializer", StringDeserializer.class.getName());
        group_one.put("bootstrap.servers","192.168.0.99:9092,192.168.0.99:9093,192.168.0.99:9094");
        group_one.put("group.id","repeat-group");
        //关闭自动提交偏移量
        group_one.put("enable.auto.commit",false);
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(group_one);
        //订阅消息
        consumer.subscribe(Collections.singletonList("repeat-topic"));

        KafkaOffset kafkaOffset = null;
        try {
            while (true){
                ConsumerRecords<String, String> records = consumer.poll(100);
                for(ConsumerRecord<String, String> consumerRecord: records){

                    kafkaOffset = new KafkaOffset();
                    kafkaOffset.setConsumer("consumer-2");
                    kafkaOffset.setTopic(consumerRecord.topic());
                    kafkaOffset.setTopicPartition(consumerRecord.partition());
                    kafkaOffset.setKey(consumerRecord.key());
                    kafkaOffset.setMessageValue(consumerRecord.value());
                    kafkaOffset.setPartitionOffset((int) consumerRecord.offset());
                    kafkaOffsetMapper.insert(kafkaOffset);
                }
                consumer.commitSync();
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            consumer.close();
        }

    }

}
