package com.nxs.kafka.sdk;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.*;

/**
 * Created by songyu on 2018/8/28.
 */
public class KafkaConsumerSDK {

    private String bootstrapServer;
    private String topic;
    private KafkaConsumer<String, String> consumer;
    private String readType;//读取的类型：1 从头开始读取 2 从最新的开始读取
    private String username;//用户名
    private String password;//密码

    public KafkaConsumerSDK(String bootstrapServer, String topic, String readType, String username, String password) {
        this.bootstrapServer = bootstrapServer;
        this.topic = topic;
        this.readType = readType;
        this.username = username;
        this.password = password;
    }

    private void initKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServer);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        if(this.readType.equals("1")){
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        }else{
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        }

//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
//        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_DOC, "");
//        props.put("acks", "all");
//        props.put("retries", 0);
//        props.put("batch.size", 163840);
//        props.put("linger.ms", 1);
//        props.put("buffer.memory", 335544320);
        
        props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "earliest");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG, "com.nxs.kafka.utils.KafkaPlainLoginModule required "
                + "serviceName=\"kafka\" "
                + "username=\""+username+"\" "
                + "password=\""+password+"\" "
                		+ ";");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        this.consumer = consumer;
    }

    /**
     * 从kafka读取消息
     *
     * @return
     */
    public List<String> read() {
        if (this.consumer == null) {
            initKafkaConsumer();
        }
//        List<TopicPartition> list = new ArrayList<TopicPartition>();
//        TopicPartition tp = new TopicPartition(this.topic, 0);
//        list.add(tp);
//        consumer.assign(list);
//        consumer.seek(tp, 0);
        this.consumer.subscribe(Arrays.asList(this.topic));
        ConsumerRecords<String, String> records = this.consumer.poll(1000);
        List<String> result = new ArrayList<String>();
        for (ConsumerRecord<String, String> record : records) {
            System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            result.add(record.value());
        }
        this.consumer.commitAsync();
//        this.consumer.close();
        return result;
    }

    /**
     * 关闭连接
     */
    public void close() {
        this.consumer.close();
    }
}
