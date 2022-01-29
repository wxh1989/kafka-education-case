package com.example.kafka.serializer;


import com.example.kafka.avro.User;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

public class AvroSerializer implements Serializer<User> {
    @Override
    public void configure(Map configs, boolean isKey) {

    }

    public byte[] serialize(String topic, User data ) {
        if(data == null) {
            System.out.println("序列化数据是空");
            return null;
        }
        DatumWriter<User> writer = new SpecificDatumWriter<>(data.getSchema());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
        try {
            writer.write(data, encoder);
        }catch (IOException e) {
            throw new SerializationException(e.getMessage());
        }
        return out.toByteArray();
    }


    @Override
    public byte[] serialize(String topic, Headers headers, User data) {
        return new byte[0];
    }

    @Override
    public void close() {

    }
}
