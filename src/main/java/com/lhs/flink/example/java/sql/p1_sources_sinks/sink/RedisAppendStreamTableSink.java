package com.lhs.flink.example.java.sql.p1_sources_sinks.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/4/22
 **/
public class RedisAppendStreamTableSink implements AppendStreamTableSink<Row> {
    private TableSchema schema;
    @Override
    public void emitDataStream(DataStream<Row> dataStream) {
        dataStream.print();
    }

    @Override
    public TableSink<Row> configure(String[] strings, TypeInformation<?>[] typeInformations) {
        return null;
    }

    @Override
    public TableSchema getTableSchema() {
        TableSchema.Builder field = TableSchema.builder()
                .field("k", DataTypes.STRING())
                .field("f", DataTypes.STRING())
                .field("v", DataTypes.STRING());
        schema = field.build();
        return schema;
    }

    @Override
    public DataType getConsumedDataType() {
        return schema.toRowDataType();
    }
}
