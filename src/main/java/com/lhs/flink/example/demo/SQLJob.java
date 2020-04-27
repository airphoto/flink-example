package com.lhs.flink.example.demo;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/4/21
 **/
public class SQLJob {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        TableEnvironment tableEnvironment = TableEnvironment.create(settings);

        List<String> allLines = Files.readAllLines(Paths.get("E:\\idea\\git\\flink\\src\\main\\resources\\q1.sql"));

        String[] lines = allLines.stream().filter((str)->!str.contains("--")).reduce((x1,y1)-> x1.trim()+" "+y1.trim()).get().split(";");

        for (String string : lines){
            System.out.println(string);
            if (string.trim().toLowerCase().startsWith("set")){
                System.out.println("setting");
                String[] kv = string.trim().split(" ")[1].split("=");
                tableEnvironment.getConfig().getConfiguration().setString(kv[0],kv[1]);
            }

            if (string.trim().toLowerCase().startsWith("create")){
                System.out.println("create");
                tableEnvironment.sqlUpdate(string.trim());
            }

            if(string.trim().toLowerCase().startsWith("insert")){
                System.out.println("insert");
                tableEnvironment.sqlUpdate(string.trim());
            }
        }

        tableEnvironment.execute("table job");
    }

}
