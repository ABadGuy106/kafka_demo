package com;

import com.nxs.kafka.sdk.KafkaConsumerSDK;

import java.util.List;

/**
 * Created by songyu on 2018/8/29.
 */
public class ConsumerTest {
    public static void main(String[] args) {
//        //cdc开发
//        String bootstrap = "172.22.2.118:9092,172.22.2.119:9092,172.22.2.120:9092";
//        String topic = "file_test2";
//        String username="alice";
//        String password="JUG93tYK30NiyLww4/EtJQ==";

    	//sasl zookeeper不认证
    	 String bootstrap = "172.22.2.118:8092,172.22.2.119:8092,172.22.2.120:8092";
////         String topic = "CANAL_CANAL_CANAL_DATA_EXCHANGE_TEST_ALL";
////    	 String topic = "CDC_YZF_SJYH_NESP1_DSG_ALL";
//    	 String topic = "DATA_EXCHANGE_CANAL";
//         String username="alice";
//         String password="JUG93tYK30NiyLww4/EtJQ==";
         
//	      String bootstrap = "172.22.3.68:8092,172.22.3.69:8092,172.22.3.70:8092";
	      String topic = "test";
//	      String topic = "DATA_EXCHANGE_CANAL";
//	      String topic = "CANAL_CANAL_CANAL_DATA_EXCHANGE_TEST_ALL";
	      String username="alice";
	      String password="JUG93tYK30NiyLww4/EtJQ==";
	      
         
//        //cdc开发
//        String bootstrap = "172.22.2.118:10092,172.22.2.119:10092,172.22.2.120:10092";
//        String topic = "test";
//        String username="alice";
//        String password="JUG93tYK30NiyLww4/EtJQ==";


//        //测试环境
//        String bootstrap = "172.22.3.68:9092,172.22.3.69:9092,172.22.3.70:9092";
//        String topic = "nxy-cdc";
////        String topic = "canal_test";
//        String username="producer";
//        String password="w0xTQoGlKUOpcdcY3jUTRg==";

        KafkaConsumerSDK kafkaConsumerSDK = new KafkaConsumerSDK(bootstrap,topic, "1",username, password);
        while(true){
        List<String> list = kafkaConsumerSDK.read();
        for(String str:list){
            System.out.println("str:"+str);
        }
            System.out.println("sleep 1s ");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
