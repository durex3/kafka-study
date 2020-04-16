package com.durex.kafkastudy.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import java.util.Map;

/**
 * @author gelong
 * @date 2020/4/15 23:31
 */
public class PartitionSimple implements Partitioner {


    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        /*
        key-1
        key-2
         */
        String keyStr = (String)key;
        String keyInt = keyStr.substring(4);
        System.out.println("keyStr: " + keyStr + ", " + "keyInt: " + keyInt);
        return Integer.parseInt(keyInt) % 2;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
