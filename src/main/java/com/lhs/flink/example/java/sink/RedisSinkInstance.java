package com.lhs.flink.example.java.sink;

import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/4/14
 **/
public class RedisSinkInstance<IN> extends RichSinkFunction<IN> implements CheckpointedFunction{

    @Override
    public void close() throws Exception {
        super.close();
        System.out.println("close");
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        System.out.println("open");
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        System.out.println(((Tuple2<String, Long>) value));
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

    }
}
