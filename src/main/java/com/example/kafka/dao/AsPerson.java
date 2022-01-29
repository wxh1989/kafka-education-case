package com.example.kafka.dao;

import com.baomidou.mybatisplus.annotation.*;
import lombok.Data;

import java.util.Date;

@Data
public class AsPerson {
    /**
     *
     * AUTO(0) 主键自增类型策略 ，数据库同时设置成主键自增
     * NONE(1), 没有主键
     * INPUT(2), 手动输入值
     * 最好使用 分布式的雪花算法
     * ASSIGN_ID(3),(雪花算法) 分配ID (主键类型为number或string）,
     * 默认实现类 com.baomidou.mybatisplus.core.incrementer.DefaultIdentifierGenerator
     * ASSIGN_UUID(4); uuid 策略
     */


    @TableId(type = IdType.ASSIGN_ID )
    private String id ;
    private String iid;
    private String cusecode;


    private String cusepassword;

    //创建时间
    /**
     * 字段自动填充方式
     * 1、在字段上添加朱姐  @TableField
     * 2、注解中有一个参数 fill 参数 可选
     * （DEFAULT 默认不处理 INSERT 插入时候 自定填充,UPDATE 更新时自动填充,INSERT_UPDATE 插入或更新时都填充）
     * 3、实现 MateObjectHandel 接口
     *
     */
    @TableField(fill = FieldFill.INSERT)
    private Date dmaker;
    //修改时间
    @TableField(fill = FieldFill.UPDATE)
    private Date dmodify;

    /**
     * 乐观锁使用字段
     * 乐观锁原理在我的 github zookeeper 教程中有详细说明
     */
    //@version 乐观锁注解
    @Version
    private int version;

    //逻辑删除字段
    @TableLogic
    private Boolean deleted;
}
