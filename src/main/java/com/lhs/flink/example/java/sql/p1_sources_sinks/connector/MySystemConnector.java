package com.lhs.flink.example.java.sql.p1_sources_sinks.connector;

import org.apache.flink.table.descriptors.ConnectorDescriptor;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/4/23
 **/
public class MySystemConnector extends ConnectorDescriptor {

    public final boolean isDebug;

    public MySystemConnector(boolean isDebug) {
        super("my-system", 1, false);
        this.isDebug = isDebug;
    }
    @Override
    protected Map<String, String> toConnectorProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("connector.debug", Boolean.toString(isDebug));
        return properties;
    }
}
