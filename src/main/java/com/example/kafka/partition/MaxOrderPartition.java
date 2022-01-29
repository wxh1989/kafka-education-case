package com.example.kafka.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

/**
 * 这是一个自定义分区例子
 */
public class MaxOrderPartition implements Partitioner {
    /**
     * 在此方法内自动以分区规则
     * @param topic 主题名称
     * @param key 分区的键（如果没有键，则为 null）
     * @param keyBytes 要分区的序列化键（如果没有键，则为 null）
     * @param value 要分区的值或 null
     * @param valueBytes 要分区的序列化值或 null
     * @param cluster 当前集群元数据
     * @return
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // cluster.partitionsForTopic(topic) 获取指定主题的分区列表
        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
        int numPartitions = partitionInfos.size();

        if(keyBytes != null || !(key instanceof String)){
            new InvalidRecordException("we expect all message to have customer name as key");
        }
        if("maxOrder".equals( key)){
            //分配到最后一个分区
            return numPartitions;
        }
        //其余的记录被散列到其他分区
        //Utils.murmur2 从字节数据生成32位Hash值
        return (Math.abs(Utils.murmur2(keyBytes))) % (numPartitions - 1);
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {
        System.out.println("Partitioner set "+map);
    }
}
