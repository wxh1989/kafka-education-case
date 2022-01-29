package com.example.kafka;

import com.example.kafka.avro.User;
import com.example.kafka.dao.Student;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class KafkaAvroTest {
    /**
     * 使用 avro String 发送消息
     */
    @Test
    public void  sendAvroMessageBySchemaString(){

        Properties kafkaProperties = new Properties();
        kafkaProperties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        System.out.println(KafkaAvroSerializer.class.getName());
        kafkaProperties.put("value.serializer",KafkaAvroSerializer.class.getName());
        kafkaProperties.put("bootstrap.servers","192.168.0.99:9092,192.168.0.99:9093,192.168.0.99:9094");
        kafkaProperties.put("schema.registry.url","http://192.168.0.99:8888");

        Producer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(kafkaProperties);
        Schema schema = User.SCHEMA$;

        GenericRecord user = new GenericData.Record(schema);
        user.put("name","sss");
        user.put("favorite_number",100);

        ProducerRecord<String,GenericRecord> record = new ProducerRecord<>("schema-registry","user",user);
        try {
            RecordMetadata metadata = producer.send(record).get();
            System.out.println(metadata.topic());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }


    }

    /**
     * 使用 实体类发送 Avro 消息
     */
    @Test
    public void  sendAvroMessageByEntity(){

        Properties kafkaProperties = new Properties();
        kafkaProperties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        System.out.println(KafkaAvroSerializer.class.getName());
        kafkaProperties.put("value.serializer",KafkaAvroSerializer.class.getName());
        kafkaProperties.put("bootstrap.servers","192.168.0.99:9092,192.168.0.99:9093,192.168.0.99:9094");
        kafkaProperties.put("schema.registry.url","http://192.168.0.99:8888");

        User user = User.newBuilder().setName("build-name-wxh").setFavoriteColor("buildColor").setFavoriteNumber(300).build();
        Producer<String, User> producer = new KafkaProducer<String, User>(kafkaProperties);

        ProducerRecord<String,User> record = new ProducerRecord<>("schema-registry","user",user);
        try {
            RecordMetadata metadata = producer.send(record).get();
            System.out.println(metadata.topic());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }


    }

    /**
     * 客户端 反序列化 Avro
     */
    @Test
    public void deserializerByAvro(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.0.99:9092,192.168.0.99:9093,192.168.0.99:9094");
        props.put("group.id", "schema-test");
        props.put("key.deserializer", StringDeserializer.class.getName());
        //设置value的反序列化类为自定义序列化类全路径
        props.put("value.deserializer", KafkaAvroDeserializer.class.getName());
        props.put("schema.registry.url", "http://192.168.0.99:8888");


        Consumer consumer = new KafkaConsumer(props);
        consumer.subscribe(Collections.singletonList("schema-registry"));

        while (true){
            ConsumerRecords<String ,User> records = consumer.poll(100);
            for(ConsumerRecord<String,User> consumerRecord : records){
                try {
                    GenericRecord user = consumerRecord.value();
                    List<Schema.Field> fields = user.getSchema().getFields();
                    Schema.Field field ;
                    for(int i = 0 ; i <fields.size(); i++){
                        field = fields.get(i);
                        System.out.println(field.name()+":"+user.get(field.name()));
                    }
                    System.out.println(user.toString());
                }catch (Exception e){
                    System.out.println(e.getMessage());
                }

            }
        }



    }

}
