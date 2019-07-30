package com.netease.ops.flink.test;

import com.alibaba.fastjson.JSON;

/**
 * @author hzjiaoguangcai
 * @time 2019/7/8 16:02
 * @city of hangzhou
 * @corp of 163.com
 */
public class LogRecord {

    private String datasource;

    private String record;

    public LogRecord() {
    }

    public LogRecord(String datasource, String record) {
        this.datasource = datasource;
        this.record = record;
    }

    public String getDatasource() {
        return datasource;
    }

    public void setDatasource(String datasource) {
        this.datasource = datasource;
    }

    public String getRecord() {
        return record;
    }

    public void setRecord(String record) {
        this.record = record;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
