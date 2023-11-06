package io.tapdata.connector.hive3;

import io.tapdata.common.CommonDbTest;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.TestItem;
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


public class Hive3Test extends CommonDbTest {
   private  Hive3Config hive3Config;

    public static final String TEST_CREATE_TABLE = "create table %s(col1 int not null)";
    private static final String HDFS_WRITE = "Hdfs Write";


    public Hive3Test(Hive3Config config, Consumer<TestItem> consumer) {
        super(config, consumer);
        this.hive3Config  = config;
        jdbcContext = new Hive3JdbcContext(config);
    }

    @Override
    public Boolean testOneByOne() {
        testFunctionMap.put("testHdfs", this::testHdfs);
        return super.testOneByOne();
    }



    @Override
    public Boolean testWritePrivilege() {
        List<String> sqls = new ArrayList<>();
        try {
            String schemaPrefix = EmptyKit.isNotEmpty(hive3Config.getDatabase()) ? (hive3Config.getDatabase() + ".") : "";
            if (jdbcContext.queryAllTables(Arrays.asList(schemaPrefix + TEST_WRITE_TABLE, (schemaPrefix + TEST_WRITE_TABLE).toUpperCase())).size() > 0) {
                sqls.add(String.format(TEST_DROP_TABLE, schemaPrefix + TEST_WRITE_TABLE));
            }
            //create
            sqls.add(String.format(TEST_CREATE_TABLE, schemaPrefix + TEST_WRITE_TABLE));
            //drop
            sqls.add(String.format(TEST_DROP_TABLE, schemaPrefix + TEST_WRITE_TABLE));
            jdbcContext.batchExecute(sqls);
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
            URI uri = new URI(hive3Config.getHdfsAddr());
            Configuration conf = new Configuration();
            conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
            fs = FileSystem.get(uri, conf, hive3Config.getUser());
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
