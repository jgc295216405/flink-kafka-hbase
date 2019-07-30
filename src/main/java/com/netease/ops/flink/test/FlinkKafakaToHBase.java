package com.netease.ops.flink.test;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;
/**
 * @author hzjiaoguangcai
 * @time 2019/7/8 16:02
 * @city of hangzhou
 * @corp of 163.com
 */
public class FlinkKafakaToHBase {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 非常关键，一定要设置启动检查点！！
        env.enableCheckpointing(5000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "nss-kafka-test1.service.163.org:9092,nss-kafka-test1.service.163.org:9093");
        props.setProperty("group.id", "athena-test.0");
        //    args[0] = "test-0921";  //传入的是kafka中的topic
        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<String>("athena-test", new SimpleStringSchema(), props);

        DataStream<LogRecord> keyedStream = env.addSource(consumer).map(new SplitFunction());
        keyedStream.writeUsingOutputFormat(new HBaseOutputFormat());
        //将结果打印出来
        env.execute("Flink-kafak-hbase");
    }

}
