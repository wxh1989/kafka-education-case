package com.example.kafka.serializer;

import java.nio.ByteBuffer;
import java.util.Map;

import com.example.kafka.dao.Student;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class MySerializer  implements Serializer<Student> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey){
        //不做任何配置
    }

    @Override
    public byte[] serialize(String topic, Student data){

        try{
            byte[] serializedName;//将name属性变成字节数组
            int serializedLength;//表示name属性的值的长度
            if(data == null){
                return null;
            }else{
                if(data.getName() != null){
                    serializedName = data.getName().getBytes("UTF-8");
                    serializedLength = serializedName.length;
                }else{
                    serializedName = new byte[0];
                    serializedLength = 0;

                }
            }
            //我们要定义一个buffer用来存储age的值，age是int类型，需要4个字节。还要存储name的值的长度（后面会分析为什么要存name的值的长度），用int表示就够了，也是4个字节，name的值是字符串，长度就有值来决定，所以我们要创建的buffer的大小就是这些字节数加在一起：4+4+name的值的长度
            ByteBuffer buffer = ByteBuffer.allocate(4+4+serializedLength);
            //put()执行完之后，buffer中的position会指向当前存入元素的下一个位置
            buffer.putInt(data.getAge());
            //由于在读取buffer中的name时需要定义一个字节数组来存储读取出来的数据，但是定义的这个数组的长度无法得到，所以只能在存name的时候也把name的长度存到buffer中
            buffer.putInt(serializedLength);
            buffer.put(serializedName);
            return buffer.array();


        }catch(Exception e){
            throw new SerializationException("error when serializing..."+e);
        }
    }

    @Override
    public void close(){
        //不需要关闭任何东西
    }


}
