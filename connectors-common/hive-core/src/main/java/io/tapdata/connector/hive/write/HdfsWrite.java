package io.tapdata.connector.hive.write;

import io.tapdata.connector.hive.HiveJdbcContext;
import io.tapdata.connector.hive.config.HiveConfig;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class HdfsWrite extends HiveJdbcWrite{

    private  final  static  String  FILE_NAME = "Hive3.txt";
    private String LOAD_SQL = "load data  inpath %s  into table %s";

    private HiveConfig hiveConfig;
    protected HiveJdbcContext hiveJdbcContext;

    protected FileSystem fs;


    public HdfsWrite(HiveJdbcContext hiveJdbcContext, HiveConfig hiveConfig) {
        super(hiveJdbcContext,hiveConfig);
        this.hiveConfig = hiveConfig;
        this.hiveJdbcContext = hiveJdbcContext;
        initFs();
    }

    public void initFs(){
        try {
            URI uri = new URI(hiveConfig.getHdfsAddr());
            Configuration conf = new Configuration();
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            fs = FileSystem.get(uri, conf, hiveConfig.getUser());
        }catch (Exception e){
            throw new RuntimeException(e);
        }

    }

    public WriteListResult<TapRecordEvent> writeRecord(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents) throws Throwable {

        WriteListResult<TapRecordEvent> writeListResult = new WriteListResult<>(0L, 0L, 0L, new HashMap<>());
        TapRecordEvent errorRecord = null;
        String context = "";
        try {
            for (TapRecordEvent tapRecordEvent : tapRecordEvents) {
                try {
                    if (tapRecordEvent instanceof TapInsertRecordEvent) {
                        context = handleInsertAndUpdate(tapRecordEvent, tapTable, context);
                        writeListResult.incrementInserted(1);
                    } else if (tapRecordEvent instanceof TapUpdateRecordEvent) {
                        context = handleInsertAndUpdate(tapRecordEvent, tapTable, context);
                        writeListResult.incrementModified(1);
                    } else if (tapRecordEvent instanceof TapDeleteRecordEvent) {
                    } else {
                        writeListResult.addError(tapRecordEvent, new Exception("Event type \"" + tapRecordEvent.getClass().getSimpleName() + "\" not support: " + tapRecordEvent));
                    }
                } catch (Throwable e) {
                    errorRecord = tapRecordEvent;
                    throw e;
                }
            }
            commitData(tapTable, context);
        } catch (Throwable e) {
            writeListResult.setInsertedCount(0);
            writeListResult.setModifiedCount(0);
            writeListResult.setRemovedCount(0);
            if (null != errorRecord) writeListResult.addError(errorRecord, e);
            throw e;
        }
        return writeListResult;

    }


    private String handleInsertAndUpdate(TapRecordEvent tapRecordEvent, TapTable tapTable, String context) {
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        Map<String, Object> after = getAfter(tapRecordEvent);
        String txtContent = "";
        for (String fieldName : nameFieldMap.keySet()) {
            txtContent = txtContent + after.get(fieldName) + "\t";
        }
        txtContent = txtContent.substring(0, txtContent.length() - 1);
        if (StringUtils.isEmpty(context)) {
            context += txtContent;
        } else {
            context = context + "\n" + txtContent;
        }
        return context;
    }


    private String writeFile(String txtContent) throws Exception {
        FSDataOutputStream outputStream = null;
        Path filePath;
        try {
            URI uri = new URI(hiveConfig.getHdfsAddr());
            String name = UUID.randomUUID().toString();

            // 指定要写入的文件路径
            String path = uri.getPath().lastIndexOf("/") > 0 ? uri.getPath() + name+FILE_NAME : uri.getPath() + "/"+FILE_NAME+name;
            filePath = new Path(path);
            // 创建文件写入流
            outputStream = fs.create(filePath);
            // 写入数据
            outputStream.write(txtContent.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
        return filePath.toString();
    }

    private int handleDelete(TapConnectorContext tapConnectorContext, TapRecordEvent tapRecordEvent, TapTable tapTable,String context) throws Throwable {
        // 先吧文件提交了
        commitData(tapTable,context);
        PreparedStatement deletePreparedStatement = getDeletePreparedStatement(tapConnectorContext, tapTable, tapRecordEvent);
        setPreparedStatementWhere(tapTable, tapRecordEvent, deletePreparedStatement, 1);
        int row;
        try {
            row = deletePreparedStatement.executeUpdate();
        } catch (Throwable e) {
            throw new Exception("Delete data failed, sql: " + deletePreparedStatement + ", message: " + e.getMessage(), e);
        }
        return row;
    }


    private void commitData(TapTable tapTable,String context) throws Exception {
        String path = "'"+writeFile(context)+"'";
        //String path = "'/user/hive/warehouse/test_tapdata.db/test3/Hive3.txt'";
        String sql = String.format(LOAD_SQL, path, hiveConfig.getDatabase() + "."+tapTable.getName());
        PreparedStatement preparedStatement = getConnection().prepareStatement(sql);
        preparedStatement.execute();
    }

    private PreparedStatement getDeletePreparedStatement(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
        PreparedStatement preparedStatement = null;
        if (null == preparedStatement) {
            DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
            String database = connectionConfig.getString("database");
            String tableId = tapTable.getId();
            LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
            if (MapUtils.isEmpty(nameFieldMap)) {
                throw new Exception("Create delete prepared statement error, table \"" + tableId + "\"'s fields is empty, retry after reload connection \"" + database + "\"'s database");
            }
            List<String> whereList = new ArrayList<>();
            Collection<String> uniqueKeys = getUniqueKeys(tapTable);
            for (String uniqueKey : uniqueKeys) {
                whereList.add("`" + uniqueKey + "`=?");
            }
            String sql = String.format(DELETE_SQL_TEMPLATE, database, tableId, String.join(" AND ", whereList));
            try {
                preparedStatement = getConnection().prepareStatement(sql);
            } catch (SQLException e) {
                throw new Exception("Create delete prepared statement error, sql: " + sql + ", message: " + e.getSQLState() + " " + e.getErrorCode() + " " + e.getMessage(), e);
            } catch (Exception e) {
                throw new Exception("Create delete prepared statement error, sql: " + sql + ", message: " + e.getMessage(), e);
            }
        }
        return preparedStatement;
    }


    protected String getKey(TapTable tapTable, TapRecordEvent tapRecordEvent) {
        Map<String, Object> after = getAfter(tapRecordEvent);
        Map<String, Object> before = getBefore(tapRecordEvent);
        Map<String, Object> data;
        if (MapUtils.isNotEmpty(after)) {
            data = after;
        } else {
            data = before;
        }
        Set<String> keys = data.keySet();
        String keyString = String.join("-", keys);
        return tapTable.getId() + "-" + keyString;
    }

}
