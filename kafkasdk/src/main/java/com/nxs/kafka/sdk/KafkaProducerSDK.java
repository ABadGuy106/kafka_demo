package com.nxs.kafka.sdk;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SaslConfigs;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Created by songyu on 2018/8/29.
 */
public class KafkaProducerSDK {
    private String bootstrapServer;
    private String topic;
    private Producer<String, String> producer;
    private String username;//用户名
    private String password;//密码

    /**
     * 参数构造方法
     * @param bootstrapServer
     * @param topic
     */
    public KafkaProducerSDK(String bootstrapServer, String topic, String username, String password){
        this.bootstrapServer = bootstrapServer;
        this.topic = topic;
        this.username = username;
        this.password = password;
    }


    /**
     * 初始化Producer
     * @return
     */
    private void initProducer(){
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServer);
        props.put("acks", "0");
        props.put("retries", 3);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG, "com.nxs.kafka.utils.KafkaPlainLoginModule required "
                + "serviceName=\"kafka\" "
                + "username=\""+username+"\" "
                + "password=\""+password+"\";");
        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props);
        this.producer = producer;
    }

    /**
     * 将message数据发送到kafka中的topic中
     * @param message
     */
    public void send(String message){
        try {
            if (this.producer == null) {
                initProducer();
            }
            Future<RecordMetadata> future = this.producer.send(new ProducerRecord<String, String>(topic, Long.toString(System.currentTimeMillis()), message));
            future.get();
            System.out.println("future.isDone():" + future.isDone());
        }catch (Exception e){
            System.out.println("**************************************************");
            e.printStackTrace();
        }
    }

    /**
     * 关闭连接
     */
    public void close(){
        this.producer.close();
    }

}
