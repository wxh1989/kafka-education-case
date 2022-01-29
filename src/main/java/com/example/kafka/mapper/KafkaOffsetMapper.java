package com.example.kafka.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.example.kafka.dao.KafkaOffset;
import org.apache.ibatis.annotations.Select;

public interface KafkaOffsetMapper extends BaseMapper<KafkaOffset> {

    @Select("select coalesce(max(partition_offset),0 ) from kafka_offset")
    int selectMaxOffset();
}
