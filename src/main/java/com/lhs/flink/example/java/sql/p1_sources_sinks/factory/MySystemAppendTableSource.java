package com.lhs.flink.example.java.sql.p1_sources_sinks.factory;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/4/23
 **/
public class MySystemAppendTableSource implements StreamTableSource<Row> {
    public MySystemAppendTableSource(boolean isDebug) {
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
        return null;
    }

    @Override
    public TableSchema getTableSchema() {
        return null;
    }
}
