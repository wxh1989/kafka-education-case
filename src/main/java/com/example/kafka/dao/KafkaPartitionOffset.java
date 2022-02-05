package com.example.kafka.dao;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class KafkaPartitionOffset {

    @TableId(type = IdType.ASSIGN_ID)
    private Long id ;
    private String topic;
    private int  form_partition;
    private int partition_offset;
    private String consumer;
    private String  consumer_group;
}
