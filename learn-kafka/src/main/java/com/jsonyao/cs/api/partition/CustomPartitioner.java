package com.jsonyao.cs.api.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 自定义Producer Partition
 */
public class CustomPartitioner implements Partitioner {

    private AtomicInteger counter = new AtomicInteger(0);

    /**
     * 自定义Producer Partition计算Partition方法
     * @param topic
     * @param key
     * @param keyBytes
     * @param value
     * @param valueBytes
     * @param cluster
     * @return
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        Integer numOfPartition = cluster.partitionCountForTopic(topic);
        System.err.println("---- 进入自定义分区器，当前分区个数：" + numOfPartition);
        System.err.println(String.format("---- 进入自定义分区器, topic: %s, key: %s, keyBytes: %s, value: %s, valueBytes: %s, cluster: %s", topic, key, keyBytes, value, valueBytes, cluster));

        // 计算Partition
        if(keyBytes == null){
            // 自定义Hash算法: 每进入一次+1, 然后做取模
            return counter.getAndIncrement() % numOfPartition;
        }else {
            return Utils.toPositive(Utils.murmur2(keyBytes)) % numOfPartition;
        }
    }

    /**
     * 自定义Producer Partition关闭方法
     */
    @Override
    public void close() {
        System.err.println("---------------  CustomPartitioner#close(..)  ---------------");
    }

    /**
     * 自定义Producer Partition自定义方法
     * @param map
     */
    @Override
    public void configure(Map<String, ?> map) {
        System.err.println("---------------  CustomPartitioner#configure(..)  ---------------");
    }
}
