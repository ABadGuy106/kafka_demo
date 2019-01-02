package com;

import com.nxs.kafka.sdk.KafkaProducerSDK;

/**
 * Created by songyu on 2018/8/29.
 */
public class ProducerTest {
    public static void main(String[] args) {

//        //测试环境
//        String bootstrap = "172.22.3.68:9092,172.22.3.69:9092,172.22.3.70:9092";
//        String topic = "test1";
//        String username="producer";
//        String password="w0xTQoGlKUOpcdcY3jUTRg==";

//	      String bootstrap = "172.22.3.68:8092,172.22.3.69:8092,172.22.3.70:8092";
//	      String topic = "test";
//	      String username="alice";
//	      String password="JUG93tYK30NiyLww4/EtJQ==";
    	
        //开发环境
//        String bootstrap = "172.22.2.118:9092,172.22.2.119:9092,172.22.2.120:9092";
//        String topic = "db2";
//        String username="alice";
//        String password="JUG93tYK30NiyLww4/EtJQ==";
    	
      String bootstrap = "172.22.2.118:8092,172.22.2.119:8092,172.22.2.120:8092";
      String topic = "test";
      String username="alice";
      String password="JUG93tYK30NiyLww4/EtJQ==";
    	
//        KafkaProducerSDK kafkaProducerSDK = new KafkaProducerSDK(bootstrap,topic,username,password);
//        String message = "{\"sourceStructureName\":\"T1\",\"schemaname\":\"NXY-CDC\",\"businessLine\":\"wlqz\",\"appCode\":\"nesp\",\"dbName\":\"nesp1\",\"operationType\":\"DC\",\"dataTime\":\"2018-09-06 15:18:15\",\"content\":{\"sql\":\"CREATE TABLE NXY-CDC.'T1'('C1' INT,'C2' INT,'C3' INT,'C4' DOUBLE,'C5' DOUBLE,'C6' NUMBER(6,2),'C7' NUMBER(6,2),'C8' CHAR(10),'C9'  VARCHAR2(10),BUSINESSLINE VARCHAR(20),APPCODE VARCHAR(20),DBNAME VARCHAR(20),OPERATIONTYPE VARCHAR(4),DATATIME DATE) \"}}\n";
//        kafkaProducerSDK.send(message);
//        String message1 = "{\"sourceStructureName\":\"t1\",\"schemaname\":\"nxy-cdc\",\"businessLine\":\"wlqz\",\"appCode\":\"nesp\",\"dbName\":\"nesp1\",\"operationType\":\"MI\",\"dataTime\":\"2018-09-06 16:42:55\",\"content\":{\"C1\":\"33\",\"C2\":\"6\",\"C3\":\"88\",\"C4\":\"100.0\",\"C5\":\"-307.0\",\"C6\":\"100.98\",\"C7\":\"12.00\",\"C8\":\"2         \",\"C9\":\"7\"}}";
//        kafkaProducerSDK.send(message1);
//        kafkaProducerSDK.close();

//        String bootstrap = "172.22.2.118:10092,172.22.2.119:10092,172.22.2.120:10092";
//        String topic = "test";
//        String username="alice";
//        String password="JUG93tYK30NiyLww4/EtJQ==";
        
        KafkaProducerSDK kafkaProducerSDK = new KafkaProducerSDK(bootstrap,topic,username,password);
//        //test test test
//        String testC = "{\"dataTime\":\"2018-09-07 10:12:27.969\",\"businessLine\":\"test\",\"dbName\":\"test\",\"sourceStructureName\":\"T_DATA_TASK_TEST\",\"operationType\":\"DC\",\"appCode\":\"test\",\"schemaName\":\"test\",\"content\":\"CREATE TABLE T_DATA_TASK_TEST  ( \\tTASK_INST_ID    \\tINTEGER NOT NULL,\\tTASK_ID         \\tINTEGER NOT NULL,\\tTASK_INST_STATUS\\tCHARACTER(1) DEFAULT NULL,\\tFAILURE_REASON  \\tVARCHAR(1024) DEFAULT NULL,\\tBEGIN_TIME      \\tdatetime DEFAULT NULL,\\tEND_TIME        \\tdatetime DEFAULT NULL,\\tCONSTRAINT SQL180904102749380 PRIMARY KEY(TASK_INST_ID))\"}";
//        kafkaProducerSDK.send(testC);
//        String testI =
//        kafkaProducerSDK.send(testI);
//        //test1 test1 test1
//        String test1C = "{\"dataTime\":\"2018-09-07 10:12:27.969\",\"businessLine\":\"test1\",\"dbName\":\"test1\",\"sourceStructureName\":\"T_DATA_TASK_TEST1\",\"operationType\":\"DC\",\"appCode\":\"test1\",\"schemaName\":\"test1\",\"content\":\"CREATE TABLE T_DATA_TASK_TEST1  ( \\tTASK_INST_ID    \\tINTEGER NOT NULL,\\tTASK_ID         \\tINTEGER NOT NULL,\\tTASK_INST_STATUS\\tCHARACTER(1) DEFAULT NULL,\\tFAILURE_REASON  \\tVARCHAR(1024) DEFAULT NULL,\\tBEGIN_TIME      \\tdatetime DEFAULT NULL,\\tEND_TIME        \\tdatetime DEFAULT NULL,\\tCONSTRAINT SQL180904102749380 PRIMARY KEY(TASK_INST_ID))\"}";
//        kafkaProducerSDK.send(test1C);
//        String test1I = "{\"dataTime\":\"2018-09-07 10:12:27.989\",\"businessLine\":\"canal\",\"dbName\":\"test1\",\"sourceStructureName\":\"T_DATA_TASK_TEST1\",\"operationType\":\"MI\",\"appCode\":\"test1\",\"schemaName\":\"test1\",\"content\":{\"TASK_INST_STATUS\":\"0\",\"BEGIN_TIME\":\"2018-09-03 16:33:47\",\"TASK_INST_ID\":\"2\",\"TASK_ID\":\"1\"}}";
//        kafkaProducerSDK.send(test1I);
//        //test2 test2 test2
//        String test2C = "{\"dataTime\":\"2018-09-07 10:12:27.969\",\"businessLine\":\"test2\",\"dbName\":\"test2\",\"sourceStructureName\":\"T_DATA_TASK_TEST2\",\"operationType\":\"DC\",\"appCode\":\"test2\",\"schemaName\":\"test2\",\"content\":\"CREATE TABLE T_DATA_TASK_TEST2  ( \\tTASK_INST_ID    \\tINTEGER NOT NULL,\\tTASK_ID         \\tINTEGER NOT NULL,\\tTASK_INST_STATUS\\tCHARACTER(1) DEFAULT NULL,\\tFAILURE_REASON  \\tVARCHAR(1024) DEFAULT NULL,\\tBEGIN_TIME      \\tdatetime DEFAULT NULL,\\tEND_TIME        \\tdatetime DEFAULT NULL,\\tCONSTRAINT SQL180904102749380 PRIMARY KEY(TASK_INST_ID))\"}";
//        kafkaProducerSDK.send(test2C);
//        String test2I = "{\"dataTime\":\"2018-09-07 10:12:27.989\",\"businessLine\":\"test2\",\"dbName\":\"test2\",\"sourceStructureName\":\"T_DATA_TASK_TEST2\",\"operationType\":\"MI\",\"appCode\":\"test2\",\"schemaName\":\"test2\",\"content\":{\"TASK_INST_STATUS\":\"0\",\"BEGIN_TIME\":\"2018-09-03 16:33:47\",\"TASK_INST_ID\":\"2\",\"TASK_ID\":\"1\"}}";
//        kafkaProducerSDK.send(test2I);
//        for(int i=0;i<1;i++){
//        	String test1="{\"dataTime\":\"2018-09-25 10:09:46.704\",\"businessLine\":\"YZF\",\"dbName\":\"dataexchange\",\"sourceStructureName\":\"nxy_test_userinfo\",\"operationType\":\"DC\",\"appCode\":\"CANAL\",\"schemaName\":\"dataexchange\",\"content\":\"CREATE TABLE `nxy_test_userinfo`  (`ID` decimal(8, 0) NOT NULL,`yonghm` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,PRIMARY KEY (`ID`) USING BTREE) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;\"}\n";
//        	kafkaProducerSDK.send(test1);
//        	String test2="{\"dataTime\":\"2018-09-25 10:18:39.268\",\"businessLine\":\"YZF\",\"dbName\":\"dataexchange\",\"sourceStructureName\":\"nxy_test_userinfo\",\"operationType\":\"DA\",\"appCode\":\"CANAL\",\"schemaName\":\"dataexchange\",\"content\":{\"alter table nxy_test_userinfo modify column yonghm VARCHAR(32)\"}}";
//        	kafkaProducerSDK.send(test2);
//        }
        kafkaProducerSDK.send("test");
        kafkaProducerSDK.close();

    }
}
