package com.lhs.flink.example.java.p4_ttl;

import com.lhs.flink.example.java.sink.RedisSinkInstance;
import com.lhs.flink.example.java.sink.RedisSinkInstance2;
import com.lhs.flink.example.java.sink.WriteSinkInstance;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

/**
 * @author lihuasong
 * @description 描述
 * @create 2019/8/7
 **/
public class TTLCountMain {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        //web地址 http://localhost:8081/#/overview
        StreamExecutionEnvironment env = LocalStreamEnvironment.createLocalEnvironmentWithWebUI(configuration);

        env.enableCheckpointing(2000);

        FsStateBackend fsStateBackend = new FsStateBackend("file:///E:\\idea\\git\\flink\\checkpoint", false);

        env.setStateBackend(fsStateBackend);

        // kafka需要的最低配置参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("group.id","flink_group");

        FlinkKafkaConsumer010<String> source = new FlinkKafkaConsumer010<String>("canal", new SimpleStringSchema(),properties);

        DataStream<String> stream = env.addSource(source);

        SingleOutputStreamOperator<Tuple2<String, Long>> tuple2SingleOutputStreamOperator = stream.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String s) throws Exception {
                return new Tuple2<>(s, 1L);
            }
        })
                .keyBy(0)
                .flatMap(new CountSateTTL());

//        tuple2SingleOutputStreamOperator.addSink(new WriteSinkInstance<>());
        tuple2SingleOutputStreamOperator.addSink(new RedisSinkInstance2());
//        tuple2SingleOutputStreamOperator.print();

        env.execute("state count");
    }
}
