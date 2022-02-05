package com.example.kafka;

import com.example.kafka.dao.Student;
import com.example.kafka.mapper.AsPersonMapper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.zookeeper.Login;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.ExecutionException;


/**
 * 偏移量 就是 消息 在分区内的下标
 * 偏移量示例
 * enable.auto.commit 此参数控制是否 自动提交，
 * auto.commit.interval 此参数用于自动提交 ,多少时间提交一次偏移量
 */
@Slf4j
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class KafkaOffsetTest {

    @Resource
    AsPersonMapper asPersonMapper;
    @Autowired
    Producer producer;
    @Autowired
    AdminClient adminClient;

    @Test
    public void createTopic(){
        //创建分区 4个分区 4个副本
        NewTopic newTopic = new NewTopic("offset-topic",4, (short) 4);
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
     * 增加某个主题的分区（注意分区只能增加不能减少）
     * @param topicName  主题名称
     * @param number  修改数量
     */
    public void edit(String topicName,Integer number){
        Map newPartitions = new HashMap();
        //创建新的分区的结果
        newPartitions.put(topicName, NewPartitions.increaseTo(number));
        CreatePartitionsResult createPartitionsResult = adminClient.createPartitions(newPartitions);
        try {
            createPartitionsResult.all().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void editPartitions(){
        this.edit("offset-topic",3);
    }

    @Test
    public void sendMessage() throws InterruptedException {
        for(int i = 0 ; i < 50; i++){
            ProducerRecord<String,String> record = new ProducerRecord<>("offset-topic","message"+i,"value"+i);
            producer.send(record);
            Thread.sleep(10000);
        }
    }

    /**
     * 手动同步提交偏移量
     * consumer.commitSync 同步提交偏移量，此方法会一直尝试提交 直到提交成功 阻塞其他消息处理
     */
    @Test
    public void consumerSyncOffset(){

        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.0.99:9092,192.168.0.99:9093,192.168.0.99:9094");
        props.put("group.id", "offset-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", StringDeserializer.class.getName());
        //关闭自动提交偏移量
        props.put("enable.auto.commit",false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList("offset-topic"));
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(100);
            for(ConsumerRecord<String, String> consumerRecord: records){
                System.out.println(consumerRecord.toString());
            }
            try {
                consumer.commitSync();
            }catch (CommitFailedException exception){
                System.out.println(exception.getMessage());
            }
        }
    }

    /**
     * 手动异步提交
     * consumer.commitAsync
     */
    @Test
    public void consumerAsyncOffset(){

        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.0.99:9092,192.168.0.99:9093,192.168.0.99:9094");
        props.put("group.id", "offset-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", StringDeserializer.class.getName());
        //关闭自动提交偏移量
        props.put("enable.auto.commit",false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList("offset-topic"));
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(100);
            for(ConsumerRecord<String, String> consumerRecord: records){
                System.out.println(consumerRecord.toString());
            }
            try {
                //处理一批数据后 提交最后一个偏移量
                consumer.commitAsync();
            }catch (CommitFailedException exception){
                System.out.println(exception.getMessage());
            }
        }
    }


    /**
     * 从指定位置开始消费
     * seek assign 和  subscribe 冲突
     */
    @Test
    public  void seekTest(){

        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.0.99:9092,192.168.0.99:9093,192.168.0.99:9094");
        props.put("group.id", "offset-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", StringDeserializer.class.getName());
        //关闭自动提交偏移量
        props.put("enable.auto.commit",false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList("repeat-topic"));
        //从 0 号分区的 的 offset = 0 的位置开始消费
        //repeat-topic-1
        TopicPartition partition =  new TopicPartition("repeat-topic",0);
        //从指定分区的offset 开始读取数据
        consumer.assign(Arrays.asList(  partition));
        consumer.seek(partition,0);
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(100);
            for(ConsumerRecord<String, String> consumerRecord: records){
                log.info("从分区获取数据"+consumerRecord.toString());
            }
            try {
                consumer.commitSync();
            }catch (CommitFailedException exception){
                System.out.println(exception.getMessage());
            }
        }

    }


    /**
     * offsetsForTimes  按时间戳查找给定分区的偏移量
     * 从固定时间点开始消费
     */
    public void seekByTimePointTest(){

        String topicName = "repeat-topic";
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.0.99:9092,192.168.0.99:9093,192.168.0.99:9094");
        props.put("group.id", "offset-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", StringDeserializer.class.getName());
        //关闭自动提交偏移量
        props.put("enable.auto.commit",false);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);


        //从一小时前开始消费
        long fetchTime = new Date().getTime() - (1000 * 60 * 60);
        //获取所属主题的分区列表
        List<PartitionInfo> topicPartitionList = consumer.partitionsFor(topicName);

        Map<TopicPartition,Long>  partitionMap = new HashMap<>();
        topicPartitionList.forEach(partitionInfo -> {
            //key:分区信息 ,value: 时间戳
            partitionMap.put(new TopicPartition(topicName,partitionInfo.partition()),fetchTime);
        });

        //按时间戳查找给定分区的偏移量
        Map<TopicPartition, OffsetAndTimestamp> timestampMap = consumer.offsetsForTimes(partitionMap);
        //开始消费
        for(Map.Entry<TopicPartition,OffsetAndTimestamp> entry : timestampMap.entrySet()){

            TopicPartition key = entry.getKey();
            OffsetAndTimestamp value = entry.getValue();
            if(value == null || key == null){
                continue;
            }
            //拿到偏移量
            long offset = value.offset();
            consumer.assign(Arrays.asList(key));
            consumer.seek(key,offset);
        }
        //开始消费
        ConsumerRecords<String, String> records = consumer.poll(0);

        while (true){
            for(ConsumerRecord<String, String> consumerRecord: records){
                log.info("从分区获取数据"+consumerRecord.toString());
            }
            try {
                consumer.commitSync();
            }catch (CommitFailedException exception){
                System.out.println(exception.getMessage());
            }
        }
    }




}
