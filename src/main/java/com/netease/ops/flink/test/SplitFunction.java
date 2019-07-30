package com.netease.ops.flink.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
/**
 * @author hzjiaoguangcai
 * @time 2019/7/8 16:02
 * @city of hangzhou
 * @corp of 163.com
 */
public class SplitFunction extends RichMapFunction<String, LogRecord> {

    private static final List<String> BUCKETS = Collections.unmodifiableList(Arrays.asList("00", "01", "02", "03", "04", "05", "06", "07", "08", "09"));
    private static final long serialVersionUID = -8464099356287049647L;
    private static volatile AtomicInteger INC = new AtomicInteger(0);
    @Override
    public void open(Configuration parameters) throws InterruptedException {
    }
    @Override
    public LogRecord map(String message) {

        LogRecord record = transRecord(message);

        return record;
    }

    /**
     * 从kafka日志中提取log
     *
     * @param message
     * @return
     */
    private LogRecord transRecord(String message) {
        try {
            JSONObject dsObject = JSON.parseObject(message);
            String logStr = dsObject.getString("_body");
            String dataSourceStr = dsObject.getString("_task_name");
            return new LogRecord(dataSourceStr, logStr);
        } catch (Exception e) {
           e.printStackTrace();
        }
        return new LogRecord();
    }
}
