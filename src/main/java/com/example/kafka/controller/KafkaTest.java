package com.example.kafka.controller;

import com.example.kafka.avro.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@RestController
public class KafkaTest {


    @GetMapping(value = "/test")
    public void KafkaAvroTest(){

        User user = new User();
        user.setName("tom");
        user.setFavoriteColor("#xddd111");

        Properties kafkaProperties = new Properties();
        kafkaProperties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("value.serializer","com.example.kafka.serializer.AvroSerializer");
        kafkaProperties.put("bootstrap.servers","192.168.0.99:9092,192.168.0.99:9093,192.168.0.99:9094");

        Producer<String, User> producer = new KafkaProducer<>(kafkaProperties);
        producer = new KafkaProducer(kafkaProperties);

        ProducerRecord<String, User> record = new ProducerRecord<>("Avro",user.getName().toString(), user);
        RecordMetadata metadata = null;
        try {
            metadata = producer.send(record).get();
            System.out.println(metadata.topic());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
