import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.netease.ops.flink.test.NumberUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author hzjiaoguangcai
 * @time 2019/7/8 16:02
 * @city of hangzhou
 * @corp of 163.com
 */

public class HBaseClient {

    public static long getTimeStamp(byte[] rowKey) {
        Preconditions.checkArgument(rowKey != null && rowKey.length >= 15);
        long reverseTimeStamp = NumberUtil.to5byteLong(rowKey, 10);
        return NumberUtil.BYTE5_LONG_MAX - reverseTimeStamp;
    }

    public static void main(String[] args) throws IOException {
        System.out.println(new Date(1562726100000l));
        Connection connection = ConnectionFactory.createConnection();

        String tableName = "sentry:athena";
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan(Bytes.toBytes(0l), Bytes.toBytes(Long.MAX_VALUE))

                .setMaxVersions()
                .addFamily(Bytes.toBytes("cf"));
        ResultScanner resultScanner = table.getScanner(scan);
        Iterator<Result> it = resultScanner.iterator();
        while (it.hasNext()) {
            Result result = it.next();
            if (!result.isEmpty()) {
                List<Cell> cells = result.listCells();
                for (Cell cell : cells) {
                    String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                    byte[] value = CellUtil.cloneValue(cell);
                    byte[] row = CellUtil.cloneRow(cell);
                    System.out.println("row:" + getTimeStamp(row) + ",column:" + column + ",value:" + new String(value));
                }
            }

        }
    }

    @Test
    public void meter() throws InterruptedException {
        final MetricRegistry registry = new MetricRegistry();//其实就是一个metrics容器，因为该类的一个属性final ConcurrentMap<String, Metric> metrics，在实际使用中做成单例就好
        ConsoleReporter reporter = ConsoleReporter.forRegistry(registry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(1, TimeUnit.SECONDS);//从启动后的1s后开始（所以通常第一个计数都是不准的，从第二个开始会越来越准），每隔一秒从MetricRegistry钟poll一次数据
        Meter meterTps = registry.meter(MetricRegistry.name(HBaseClient.class, "request", "tps"));//将该Meter类型的指定name的metric加入到MetricsRegistry中去

        System.out.println("执行与业务逻辑");

        while (true) {
            meterTps.mark();//总数以及m1,m5,m15的数据都+1
            Thread.sleep(500);
        }
    }
}
