package com.example.kafka.callback;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ProducerCallback implements Callback {
    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if(e != null){
            e.printStackTrace();
        }else {
            System.out.println("异步消息返回的主题信息"+ recordMetadata.topic());
        }
    }
}
