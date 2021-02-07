package com.jsonyao.cs.api.serial;

import com.jsonyao.cs.api.entity.User;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Kafka自定义User序列化器
 */
public class UserSerializer implements Serializer<User> {

    /**
     * User序列化器初始化方法
     * @param map
     * @param b
     */
    @Override
    public void configure(Map<String, ?> map, boolean b) {
        System.err.println("---------------  UserSerializer#configure(..)  ---------------");
    }

    /**
     * User序列化器序列化方法
     * @param s
     * @param user
     * @return
     */
    @Override
    public byte[] serialize(String s, User user) {
        if(user == null){
            return null;
        }

        // 根据User属性构造字节数组
        String id = user.getId();
        String name = user.getName();
        byte[] idBytes, nameBytes;

        try {
            // 获取每个属性的字节数组
            if(id == null){
                idBytes = new byte[0];
            }else {
                idBytes = id.getBytes("UTF-8");
            }
            if(name == null){
                nameBytes = new byte[0];
            }else {
                nameBytes = name.getBytes("UTF-8");
            }

            // 分配需要传输的字节数组: 各属性字节数组长度 + 各属性实际字节数组
            ByteBuffer byteBuffer = ByteBuffer.allocate(4 + idBytes.length + 4 + nameBytes.length);

            // 设置ID属性: 字节数组长度 + 实际字节数组
            byteBuffer.putInt(idBytes.length);
            byteBuffer.put(idBytes);

            // 设置NAME属性: 字节数组长度 + 实际字节数组
            byteBuffer.putInt(nameBytes.length);
            byteBuffer.put(nameBytes);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        return new byte[0];
    }

    /**
     * User序列化器关闭方法
     */
    @Override
    public void close() {
        System.err.println("---------------  UserSerializer#close(..)  ---------------");
    }
}
