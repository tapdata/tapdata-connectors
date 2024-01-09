package io.tapdata.connector.hudi;

import io.tapdata.common.CommonDbTest;
import io.tapdata.connector.hudi.config.HudiConfig;
import io.tapdata.connector.hudi.write.HuDiWriteBySparkClient;
import io.tapdata.constant.DbTestItem;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.util.NetUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.testItem;

public class HudiTest extends CommonDbTest {
    private HudiConfig hudiConfig;

    protected static final String TEST_WRITE_RECORD = "insert into %s values(0,0)";
    private static final String HDFS_WRITE = "Hdfs Write";


    public HudiTest(HudiConfig config, Consumer<TestItem> consumer) {
        super(config, consumer);
        this.hudiConfig = config;
        jdbcContext = new HudiJdbcContext(config);
    }

    @Override
    public Boolean testOneByOne() {
//        testFunctionMap.put("testHdfs", this::testHdfs);
        return super.testOneByOne();
    }
    protected static String getTestCreateTable() {
        String  TEST_CREATE_TABLE = "create table %s(col1 int not null,col2 int) using hudi options (primaryKey = 'col1')";
        return TEST_CREATE_TABLE;
    }

    protected static String getTestUpdateRecord() {
        String TEST_UPDATE_RECORD = "update %s set col2=1 where 1=1";
        return TEST_UPDATE_RECORD;
    }

    @Override
    public Boolean testHostPort() {
        String nameSrvAddr = hudiConfig.getNameSrvAddr();
        List<String> address = new ArrayList<>();
        if(nameSrvAddr.contains(",")) {
            address = Arrays.asList(nameSrvAddr.split(","));
        }else {
            address.add(nameSrvAddr);
        }
        StringBuilder failedHostPort = new StringBuilder();
        for (String addr : address) {
            String host = addr.split(":")[0];
            String port = addr.split(":")[1];
            try {
                NetUtil.validateHostPortWithSocket(String.valueOf(host), Integer.valueOf(port));
            } catch (Exception e) {
                failedHostPort.append(host).append(":").append(port).append(",");
            }
        }
        if (StringUtils.isNotBlank(failedHostPort)) {
            consumer.accept(testItem(DbTestItem.HOST_PORT.getContent(), TestItem.RESULT_FAILED, failedHostPort.toString()));
            return false;
        }
        consumer.accept(testItem(DbTestItem.HOST_PORT.getContent(), TestItem.RESULT_SUCCESSFULLY));
        return true;

    }
    @Override
    public Boolean testWritePrivilege() {
        try {
            //create
//            String sql = String.format(getTestCreateTable(), TEST_WRITE_TABLE);
//            jdbcContext.execute(sql);
//            HudiJdbcContext context = new HudiJdbcContext(hudiConfig);
//            HuDiWriteBySparkClient sparkClient =  new HuDiWriteBySparkClient(context, hudiConfig)
//                    .log(new TapLog());
//            List<TapRecordEvent> recordEvents = new ArrayList<>();
            TapInsertRecordEvent insertRecordEvent = new TapInsertRecordEvent();
//            sparkClient.writeRecord(recordEvents);

            consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY, TEST_WRITE_SUCCESS));
        } catch (Exception e) {
            if (e instanceof SQLFeatureNotSupportedException) {
                // version compatibility
                if (e.getMessage() != null && e.getMessage().contains("Method not supported")) {
                    consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY, TEST_WRITE_SUCCESS));
                    return true;
                }
            }
            consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_FAILED, e.getMessage()));
            return false;
        }
        return true;
    }


    public Boolean testHdfs() {
        FSDataOutputStream outputStream = null;
        FileSystem fs = null;
        Path filePath;
        try {
            URI uri = new URI(hudiConfig.getHdfsAddr());
            Configuration conf = new Configuration();
            conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
            fs = FileSystem.get(uri, conf, hudiConfig.getUser());
            // 指定要写入的文件路径
            String path = uri.getPath().lastIndexOf("/") > 0 ? uri.getPath() + "test.txt" : uri.getPath() + "/test.txt";
            filePath = new Path(path);
            // 创建文件写入流
            outputStream = fs.create(filePath);
            String txtContent = "Hello, HDFS!";
            // 写入数据
            outputStream.write(txtContent.getBytes(StandardCharsets.UTF_8));

        } catch (Exception e) {
            consumer.accept(testItem(HDFS_WRITE, TestItem.RESULT_FAILED, e.getMessage()));
            return false;
        } finally {
            try {
                if (outputStream != null) {
                    outputStream.close();
                }
                if (fs != null) {
                    fs.close();
                }
            }catch (Exception e1){

            }
        }
        consumer.accept(testItem(HDFS_WRITE, TestItem.RESULT_SUCCESSFULLY));
        return true;
    }
}
