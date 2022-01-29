package com.example.kafka.handler;

import com.baomidou.mybatisplus.core.handlers.MetaObjectHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.reflection.MetaObject;
import org.springframework.stereotype.Component;

import java.util.Date;

@Slf4j
//关键注解 加入到spring ioc（控制（容器控制对象的生成）
// 反转（将对象 注入 到依赖类 及反转，正转 则是类自己创建对象 ））
@Component
public class MyMetaObjectHandle implements MetaObjectHandler {
    //插入时填充策略
    @Override
    public void insertFill(MetaObject metaObject) {
        log.info("start insert fill.....");
        this.setFieldValByName("dmaker",new Date(),metaObject);

    }
    //更新时填充策略
    @Override
    public void updateFill(MetaObject metaObject) {
        log.info("start update fill......");
        this.setFieldValByName("dmodify",new Date(),metaObject);

    }
}
