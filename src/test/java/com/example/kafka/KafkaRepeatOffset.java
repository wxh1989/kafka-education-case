package com.example.kafka;

import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.example.kafka.dao.KafkaOffset;
import com.example.kafka.dao.KafkaPartitionOffset;
import com.example.kafka.dao.Student;
import com.example.kafka.mapper.KafkaOffsetMapper;
import com.example.kafka.mapper.KafkaPartitionOffsetMapper;
import com.example.kafka.saveoffset.SaveOffsetOnRebalance;
import com.example.kafka.serializer.MyDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.checkerframework.checker.units.qual.A;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * 消费者重复消费消息的场景
 */
@Slf4j
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class KafkaRepeatOffset {

    //spring 手动事务
    @Autowired
    DataSourceTransactionManager dataSourceTransactionManager;
    @Autowired
    TransactionDefinition transactionDefinition;

    @Resource
    private KafkaOffsetMapper kafkaOffsetMapper;
    @Resource
    private KafkaPartitionOffsetMapper kafkaPartitionOffsetMapper;
    @Autowired
    private AdminClient adminClient;
    @Autowired
    private Producer producer;


    //此用例的 主题名称
    private static final String  TOPIC_NAME = "test-repeat-topic";

    private static final String CONSUMER_GROUP_NAME = "achieve_group";
    @Test
    /**
     * 创建主题
     */
    public void createRepeatTopic(){
        //创建分区 4个分区 4个副本
        NewTopic newTopic = new NewTopic(TOPIC_NAME,2, (short) 3);
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
        newPartitions.put(TOPIC_NAME, NewPartitions.increaseTo(2));
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
        for(int i = 0 ;i < 100; i++){
            ProducerRecord<String,String> record =
                    new ProducerRecord<>(TOPIC_NAME,
                            "key"+i,
                            "这是第"+i+"条消息");
            try {
                Thread.sleep(5000);
                System.out.println(producer.send(record).get().toString());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 复现消费者重复消费的场景
     */
    @Test
    public void achieveRepeat(){
        Properties group_one =  new Properties();
        group_one.put("key.deserializer", StringDeserializer.class.getName());
        group_one.put("value.deserializer", StringDeserializer.class.getName());
        group_one.put("bootstrap.servers","192.168.0.99:9092,192.168.0.99:9093,192.168.0.99:9094");
        group_one.put("group.id",CONSUMER_GROUP_NAME);
        //关闭自动提交偏移量
        group_one.put("enable.auto.commit",false);

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(group_one);
        //订阅消息
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
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
                    if(count > 3){
//                        log.error("#######没来得及提交偏移量,系统挂掉了#############");
//                        System.exit(0);
                    }
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
     * 解决重复消费问题 数据的一致性 高于 吞吐量 使用此方案
     *
     * 要解决 重复 消费的问题 就要保证 数据的原子一致性
     * 要将 数据 及 偏移量都记录到数据库中
     * 下次 从数据库中 取出 最新的偏移量，从最新的偏移量处开始 消费
     * 数据的 可靠性不要求非常高，不建议使用此方案，这样效率会降低，
     * 应为每条消息都要和数据库交互
     */
    @Test
    public void  solveRepeat(){

        TransactionStatus transactionStatus = dataSourceTransactionManager.getTransaction(transactionDefinition);

        Properties group_one =  new Properties();
        group_one.put("key.deserializer", StringDeserializer.class.getName());
        group_one.put("value.deserializer", StringDeserializer.class.getName());
        group_one.put("bootstrap.servers","192.168.0.99:9092,192.168.0.99:9093,192.168.0.99:9094");
        group_one.put("group.id",CONSUMER_GROUP_NAME);
        /*
           当主题 是一个新的消费组 或者 offset 的消费方式，offset 不存在
         * latest(默认): 只消费自己启动之后发送到主题的消息
         * earliest: 第一次从头开始消费，之后按offset记录继续消费 区别于 seekToBeginning (这个方法是一直都从头开始消费)
         */
//        group_one.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //关闭自动提交偏移量
        group_one.put("enable.auto.commit",false);

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(group_one);
        //订阅消息


        consumer.subscribe(Collections.singletonList(TOPIC_NAME),new SaveOffsetOnRebalance(consumer,kafkaPartitionOffsetMapper,dataSourceTransactionManager,transactionStatus));


        KafkaOffset kafkaOffset = null;
        int count = 0;
        try {
            while (true){
                ConsumerRecords<String, String> records = consumer.poll(0);
                for(ConsumerRecord<String, String> consumerRecord: records){
                    //业务数据
                    kafkaOffset = new KafkaOffset();
                    kafkaOffset.setConsumer("consumer-2");
                    kafkaOffset.setTopic(consumerRecord.topic());
                    kafkaOffset.setTopicPartition(consumerRecord.partition());
                    kafkaOffset.setKey(consumerRecord.key());
                    kafkaOffset.setMessageValue(consumerRecord.value());
                    kafkaOffset.setPartitionOffset((int) consumerRecord.offset());
                    kafkaOffsetMapper.insert(kafkaOffset);
                    //提交偏移量到数据库中

                    KafkaPartitionOffset kafkaPartitionOffset  = new KafkaPartitionOffset();
                    kafkaPartitionOffset.setTopic(TOPIC_NAME);
                    kafkaPartitionOffset.setForm_partition(consumerRecord.partition());
                    kafkaPartitionOffset.setPartition_offset(count);
                    kafkaPartitionOffset.setConsumer_group(CONSUMER_GROUP_NAME);
                    kafkaPartitionOffset.setConsumer("consumer-2");


                    //更新条件
                    HashMap updateMap = new HashMap();
                    updateMap.put("topic",TOPIC_NAME);
                    updateMap.put("form_partition",consumerRecord.partition());
                    UpdateWrapper updateWrapper = new UpdateWrapper();
                    updateWrapper.allEq(updateMap);

                    Long databaseCount = kafkaPartitionOffsetMapper.selectCount(updateWrapper);
                    if(databaseCount == 0){
                        kafkaPartitionOffsetMapper.insert(kafkaPartitionOffset);
                    }else {
                        kafkaPartitionOffsetMapper.update(kafkaPartitionOffset,updateWrapper);
                    }
                    count++;
                    if(count > 10){
                        //这种程序直接挂掉的 是无法进入在均衡监听器中的
//                        System.exit(0);
                        //程序中 出现异常中断了 当前操作 会进入 在均衡监听器
//                        throw  new ArrayIndexOutOfBoundsException();
                    }
                }
                if (records.count() > 0 ){
                    //不在将offset提交到kafka 而是直接 保存到数据库中
//                    consumer.commitSync();
                    dataSourceTransactionManager.commit(transactionStatus);
                }
            }
        }catch (Exception e){
//            dataSourceTransactionManager.rollback(transactionStatus);
            e.printStackTrace();
        }finally {
            consumer.close();
        }

    }




}
