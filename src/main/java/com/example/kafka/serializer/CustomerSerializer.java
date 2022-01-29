package com.example.kafka.serializer;

import com.example.kafka.dao.Customer;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class CustomerSerializer implements Serializer<Customer> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Customer data) {
        byte [] serializeName ;
        int stringSize;
        if(data == null){
            return  null;
        }else {
            if(data.getCustomerName() != null){
                serializeName = data.getCustomerName().getBytes(StandardCharsets.UTF_8);
                stringSize = serializeName.length;
            }else{
                serializeName = new byte[0];
                stringSize = 0;
            }
            //我们要定义一个buffer用来存储age的值，
            // age是int类型，需要4个字节。
            // 还要存储name的值的长度（后面会分析为什么要存name的值的长度），
            // 用int表示就够了，也是4个字节，name的值是字符串，长度就有值来决定
            // ，所以我们要创建的buffer的大小就是这些字节数加在一起：4+4+name的值的长度

            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + stringSize);
            //put()执行完之后，buffer中的position会指向当前存入元素的下一个位置
            buffer.putInt(data.getCustomerId());
            //由于在读取buffer中的name时需要定义一个字节数组来存储读取出来的数据，但是定义的这个数组的长度无法得到，
            // 所以只能在存name的时候也把name的长度存到buffer中
            buffer.putInt(stringSize);
            buffer.put(serializeName);
            return buffer.array();
        }
    }

    @Override
    public void close() {

    }
}
