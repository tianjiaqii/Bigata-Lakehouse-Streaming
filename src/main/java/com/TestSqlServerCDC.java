package com;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;

import com.ververica.cdc.connectors.base.options.StartupOptions;

import java.util.Properties;

public class TestSqlServerCDC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties debeziumProperties = new Properties();
        debeziumProperties.put("snapshot.mode","schema_only");
        debeziumProperties.put("database.history.store.only.monitored.tables.ddl","true");

        DebeziumSourceFunction<String> sqlServerSource = SqlServerSource.<String>builder()
                .hostname("cdh01")    // SQL Server 主机地址
                .port(1433)                  // SQL Server 端口（默认 1433）
                .username("sa")              // 数据库登录用户名
                .password("tjq0605*")       // 数据库登录密码
                .database("test_cdc")     // 要监听的数据库名
              //  .schemaList("dbo")      // 架构名
                .tableList("dbo.test")   // 要监听的表（格式：模式.表名）
                .startupOptions(StartupOptions.earliest()) // 从最新的变更开始消费（也可配初始全量+增量）
                //StartupOptions.latest()
                .debeziumProperties(debeziumProperties) // 传入 Debezium 配置
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 CDC 数据转成 JSON 字符串
                .build();


        DataStreamSource<String> dataStreamSource = env.addSource(sqlServerSource, "test_source");
        dataStreamSource.print().setParallelism(1);
        env.execute("TestSqlServerCDC");
    }
}
