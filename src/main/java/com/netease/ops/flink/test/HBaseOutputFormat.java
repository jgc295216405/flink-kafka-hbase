package com.netease.ops.flink.test;


import org.apache.flink.api.common.io.OutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
/**
 * @author hzjiaoguangcai
 * @time 2019/7/8 16:02
 * @city of hangzhou
 * @corp of 163.com
 */
public class HBaseOutputFormat implements OutputFormat<LogRecord> {
private static final Logger LOGGER= LoggerFactory.getLogger(HBaseOutputFormat.class);
    private Connection conn = null;
    private Table table = null;

    @Override
    public void configure(org.apache.flink.configuration.Configuration configuration) {

    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        this.conn = ConnectionFactory.createConnection(configuration);
        table = conn.getTable(TableName.valueOf("sentry:athena"));
    }

    @Override
    public void writeRecord(LogRecord record) throws IOException {
        Put put = new Put(Bytes.toBytes(record.getDatasource()));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(record.getDatasource()+"-jgc-"+new Date()), Bytes.toBytes(record.getRecord()));
        table.put(put);
        LOGGER.info("aaa:"+record.toString());
    }

    @Override
    public void close() throws IOException {
        if (table != null) {
            table.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

}
