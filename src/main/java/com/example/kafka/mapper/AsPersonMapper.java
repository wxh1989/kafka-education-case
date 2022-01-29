package com.example.kafka.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.example.kafka.dao.AsPerson;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.HashMap;
import java.util.List;

public interface AsPersonMapper extends BaseMapper<AsPerson> {
    /**
     * 和Mybatis使用方法一致
     * @param name
     * @return
     */
    List<HashMap> selectByCorp(@Param("icorp") String name);

    @Select("select as_role.iid,as_role.cname,ar.imenu from as_role\n" +
            "left join as_rolemenu ar on as_role.iid = ar.irole where ar.imenu = #{imenu}")
    List<HashMap> selectByDynamicSql( HashMap param);

}
