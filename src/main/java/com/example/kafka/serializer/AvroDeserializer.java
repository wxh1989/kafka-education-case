package com.example.kafka.serializer;

import com.example.kafka.avro.User;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

public class AvroDeserializer implements Deserializer<User> {
    @Override
    public void configure(Map configs, boolean isKey) {

    }


    @Override
    public User deserialize(String topic, byte[] data) {
        if(data == null) {
            return null;
        }
        User user = new User();
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        DatumReader<User> userDatumReader = new SpecificDatumReader<>(user.getSchema());
        BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(in, null);
        try {
            user = userDatumReader.read(null, decoder);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return user;
    }

    @Override
    public User deserialize(String topic, Headers headers, byte[] data) {
        return null;
    }

    @Override
    public void close() {

    }
}
