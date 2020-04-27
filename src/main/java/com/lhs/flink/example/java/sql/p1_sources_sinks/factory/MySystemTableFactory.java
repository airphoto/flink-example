package com.lhs.flink.example.java.sql.p1_sources_sinks.factory;

import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/4/23
 **/
public class MySystemTableFactory implements StreamTableSourceFactory<Row> {
    @Override
    public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
        boolean isDebug = Boolean.valueOf(properties.get("connector.debug"));

        return new MySystemAppendTableSource(isDebug);
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put("update-mode", "append");
        context.put("connector.type", "my-system");
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> list = new ArrayList<>();
        list.add("connector.debug");
        return list;
    }
}
