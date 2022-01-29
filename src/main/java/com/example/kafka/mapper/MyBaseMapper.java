package com.example.kafka.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Param;

import java.util.HashMap;
import java.util.List;

public interface MyBaseMapper extends BaseMapper<HashMap> {

    public List<HashMap> selectDynamicSql(@Param("sql") String sql);
}
