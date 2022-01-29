package com.example.kafka;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
//mybatis-plus 扫描 Mapper 文件夹;
@MapperScan("com.example.kafka.mapper")
public class KafkaMain {

    public static void main(String[] args) {

        SpringApplication.run(KafkaMain.class,args);
    }
}
