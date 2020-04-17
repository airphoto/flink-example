package com.lhs.flink.example.java.async;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.scala.async.ResultFuture;
import org.apache.flink.streaming.api.scala.async.RichAsyncFunction;
import redis.clients.jedis.Jedis;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/4/14
 **/
public class AsyncDatabaseRequest extends RichAsyncFunction<Tuple2<String,Long>,Tuple2<String,Long>> {

    private transient Jedis jedis;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        jedis = new Jedis("",16379);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void asyncInvoke(Tuple2<String, Long> input, ResultFuture<Tuple2<String, Long>> resultFuture) {

    }
}
