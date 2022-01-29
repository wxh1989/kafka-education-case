package com.example.kafka.config;

import com.example.kafka.serializer.AvroDeserializer;
import com.example.kafka.serializer.CustomerDeserializer;
import com.example.kafka.serializer.MyDeserializer;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.*;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.*;

@Configuration
public class KafkaConfig {

    @Bean
    public Producer createKafkaProducer(){
        Producer producer = null;
        Properties kafkaProperties = new Properties();
        kafkaProperties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("bootstrap.servers","192.168.0.99:9092,192.168.0.99:9093,192.168.0.99:9094");

        producer = new KafkaProducer(kafkaProperties);
        return producer;
    }

    /**
     * 创建消费者
     * @return
     */
    @Bean
    public Consumer createKafkaConsumer(){
        Properties kafkaProperties = new Properties();
        kafkaProperties.put("key.deserializer", StringDeserializer.class.getName());
        kafkaProperties.put("value.deserializer", MyDeserializer.class.getName());
        kafkaProperties.put("bootstrap.servers","192.168.0.99:9092,192.168.0.99:9093,192.168.0.99:9094");
        kafkaProperties.put("group.id","test-group");
        Consumer consumer = new KafkaConsumer(kafkaProperties);
        return consumer;
    }
    @Bean
    public AdminClient createAdminClient(){
        Properties kafkaProperties = new Properties();

        kafkaProperties.put("bootstrap.servers","192.168.0.99:9092");
        kafkaProperties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("bootstrap.servers","192.168.0.99:9092,192.168.0.99:9093,192.168.0.99:9094");
        return AdminClient.create(kafkaProperties);
    }
}
