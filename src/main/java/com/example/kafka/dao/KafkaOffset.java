package com.example.kafka.dao;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
//全参构造
@AllArgsConstructor
//无参构造
@NoArgsConstructor
public class KafkaOffset {

    @TableId(type = IdType.ASSIGN_ID)
    private String id;

    private String topic;
    private int topicPartition;
    private String key;
    private String messageValue;
    private String consumer;
    private int partitionOffset;

}
