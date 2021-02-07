package com.jsonyao.cs.api.serial;

import com.jsonyao.cs.api.entity.User;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Kafka自定义User反序列化器
 */
public class UserDeserializer implements Deserializer<User> {

    /**
     * 自定义User反序列化器初始化方法
     * @param map
     * @param b
     */
    @Override
    public void configure(Map<String, ?> map, boolean b) {
        System.err.println("---------------  UserDeserializer#configure(..)  ---------------");
    }

    /**
     * 自定义User反序列化器反序列化方法
     * @param topic
     * @param data
     * @return
     */
    @Override
    public User deserialize(String topic, byte[] data) {
        if(data == null){
            return null;
        }
        if(data.length < 8){
            throw new SerializationException("size is wrong, must be data.length >= 8");
        }

        // 包装字节数组成ByteBuffer对象
        ByteBuffer byteBuffer = ByteBuffer.wrap(data);

        // 获取ID属性
        int idLen = byteBuffer.getInt();
        byte[] idBytes = new byte[idLen];
        byteBuffer.get(idBytes);

        // 获取NAME属性
        int nameLen = byteBuffer.getInt();
        byte[] nameBytes = new byte[nameLen];
        byteBuffer.get(nameBytes);

        // 设置String对象
        String id, name;
        try {
            id = new String(idBytes, "UTF-8");
            name = new String(nameBytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("deserializing error! ", e);
        }

        // 返回反序列化好的User对象
        return new User(id, name);
    }

    /**
     * 自定义User反序序列化器关闭方法: : 手工关闭的不会执行, 只有代码自动关闭的才会
     */
    @Override
    public void close() {
        System.err.println("---------------  UserDeserializer#close(..)  ---------------");
    }
}
