package com.lhs.flink.example.java.sql;

import com.lhs.flink.example.java.sql.p1_sources_sinks.connector.MySystemConnector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/4/23
 **/
public class MyConnectorTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.connect(new MySystemConnector(true))
                .withSchema(new Schema()
                        .field("key", DataTypes.STRING())
                        .field("v", DataTypes.STRING())
                )
                .inAppendMode()
                .createTemporaryTable("mySystemTable");

        Table table = tableEnv.from("mySystemTable");

        tableEnv.toAppendStream(table, Row.class).print();

        tableEnv.execute("self source");
    }
}
