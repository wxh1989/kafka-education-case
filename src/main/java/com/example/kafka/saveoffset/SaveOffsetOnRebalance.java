package com.example.kafka.saveoffset;

import com.baomidou.mybatisplus.core.conditions.AbstractWrapper;
import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.example.kafka.dao.KafkaPartitionOffset;
import com.example.kafka.mapper.KafkaPartitionOffsetMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.stereotype.Component;
import org.springframework.transaction.TransactionStatus;

import javax.annotation.Resource;
import java.util.*;

@Slf4j

public class SaveOffsetOnRebalance implements ConsumerRebalanceListener {


    SaveOffsetOnRebalance() {

    }

    public SaveOffsetOnRebalance(Consumer consumer, KafkaPartitionOffsetMapper kafkaPartitionOffsetMapper,
                                 DataSourceTransactionManager dataSourceTransactionManager, TransactionStatus transactionStatus) {
        this.consumer = consumer;
        this.kafkaPartitionOffsetMapper = kafkaPartitionOffsetMapper;
        this.dataSourceTransactionManager = dataSourceTransactionManager;
        this.transactionStatus = transactionStatus;
    }

    public Consumer consumer;

    public KafkaPartitionOffsetMapper kafkaPartitionOffsetMapper;
    public DataSourceTransactionManager dataSourceTransactionManager;
    public TransactionStatus transactionStatus;

    /**
     * 在均衡时，客户端会短暂的连不上kafka的 所以需要记录下offset
     *
     * 触发此方法的条件
     * 在均衡开始之前（如 ：新的consumer 加入到了分组触发在均衡）
     * 或停止读取消息 之后被调用
     *
     * @param partitions
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.warn("########消费者关闭之前");
        //提交数据库事务
        dataSourceTransactionManager.commit(transactionStatus);
    }

    /**
     * 重新分配分区之后 或者 开始读取消息之前
     * @param partitions
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

        log.warn("#######客户端连启动后的分区数量:" + partitions.size());
        partitions.forEach(item -> {
            Map con = new HashMap<>();
            con.put("topic", item.topic());
            con.put("form_partition", item.partition());
            QueryWrapper<KafkaPartitionOffset> queryWrapper = new QueryWrapper<>();
            queryWrapper.allEq(con);

            KafkaPartitionOffset partitionOffset = kafkaPartitionOffsetMapper.selectOne(queryWrapper);
            if (partitionOffset != null) {

            } else {
//                consumer.assign(Arrays.asList(item));
                log.warn("###########从指定Offset处开始读取消息");
                consumer.seek(item, 0);

            }
        });

        log.warn("###########消费者启动之后");
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {

    }
}
