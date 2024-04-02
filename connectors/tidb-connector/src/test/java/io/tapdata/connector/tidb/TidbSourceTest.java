package io.tapdata.connector.tidb;

import io.tapdata.connector.mysql.entity.MysqlBinlogPosition;
import io.tapdata.connector.tidb.cdc.*;
import io.tapdata.connector.tidb.config.TidbConfig;
import io.tapdata.constant.ConnectionTypeEnum;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import org.apache.commons.collections.MapUtils;
import org.apache.flink.core.execution.JobClient;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.catalog.Catalog;
import org.tikv.common.codec.TableCodec;
import org.tikv.common.key.RowKey;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.types.IntegerType;
import org.tikv.kvproto.Cdcpb;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.tikv.common.codec.TableCodec.decodeObjects;

public class TidbSourceTest {

   @Test
   public  void testTidbJdbcContextIsSerializable(){
       Assert.assertTrue(Serializable.class.isAssignableFrom(TidbJdbcContext.class));
   }

   @Test
    public void testTimestampToStreamOffsetOffsetIsNull() throws Throwable {
       TidbConnector tidbConnector = new TidbConnector();
       TidbJdbcContext tidbJdbcContext = Mockito.mock(TidbJdbcContext.class);
       TapConnectorContext connectorContext = Mockito.mock(TapConnectorContext.class);
       ReflectionTestUtils.setField(tidbConnector,"tidbJdbcContext",tidbJdbcContext);
       MysqlBinlogPosition mysqlBinlogPosition = new MysqlBinlogPosition();
       long expectedData = 123456789876L;
       mysqlBinlogPosition.setPosition(expectedData);
       when(tidbJdbcContext.readBinlogPosition()).thenReturn(mysqlBinlogPosition);
       Object actualData=  ReflectionTestUtils.invokeMethod(tidbConnector,"timestampToStreamOffset",connectorContext,null);
       Assert.assertEquals(expectedData >>18,actualData);

   }


   @Test
   public void testTimestampToStreamOffsetOffsetIsNotNull() throws Throwable {
      TidbConnector tidbConnector = new TidbConnector();
      TidbJdbcContext tidbJdbcContext = Mockito.mock(TidbJdbcContext.class);
      TapConnectorContext connectorContext = Mockito.mock(TapConnectorContext.class);
      ReflectionTestUtils.setField(tidbConnector,"tidbJdbcContext",tidbJdbcContext);
      long expectedData = 123456789876L;
      Object actualData=  ReflectionTestUtils.invokeMethod(tidbConnector,"timestampToStreamOffset",connectorContext,expectedData);
      Assert.assertEquals(expectedData,actualData);

   }


   @Test
   public void testTestPbserverForSource() {
      TidbConfig tidbConfig = new TidbConfig();
      tidbConfig.setHost("127.0.0.1");
      tidbConfig.setPort(2379);
      Consumer<TestItem> consumer = Mockito.mock(Consumer.class);
      ConnectionOptions connectionOptions = new ConnectionOptions();
      TidbConnectionTest tidbConnectionTest = new TidbConnectionTest(tidbConfig, consumer, connectionOptions);
      tidbConnectionTest.testOneByOne();
      Map<String, Supplier<Boolean>> testFunctionMap = (Map<String, Supplier<Boolean>>) ReflectionTestUtils.getField(tidbConnectionTest, "testFunctionMap");
      Assert.assertTrue(testFunctionMap.containsKey("testPbserver"));

   }


   @Test
   public void testTestPbserverForTarget() {
      TidbConfig tidbConfig = new TidbConfig();
      tidbConfig.setHost("127.0.0.1");
      tidbConfig.setPort(2379);
      tidbConfig.set__connectionType(ConnectionTypeEnum.TARGET.getType());
      Consumer<TestItem> consumer = Mockito.mock(Consumer.class);
      ConnectionOptions connectionOptions = new ConnectionOptions();
      TidbConnectionTest tidbConnectionTest = new TidbConnectionTest(tidbConfig, consumer, connectionOptions);
      tidbConnectionTest.testOneByOne();
      Map<String, Supplier<Boolean>> testFunctionMap = (Map<String, Supplier<Boolean>>) ReflectionTestUtils.getField(tidbConnectionTest, "testFunctionMap");
      Assert.assertTrue(!testFunctionMap.containsKey("testPbserver"));

   }


   @Test
   public void testOnStop() {
      TidbConnector tidbConnector = new TidbConnector();
      TidbConfig tidbConfig = new TidbConfig();
      AtomicBoolean started = new AtomicBoolean(true);
      TidbCdcService tidbCdcService = new TidbCdcService(tidbConfig, new TapLog(), started);
      TapConnectorContext connectorContext = Mockito.mock(TapConnectorContext.class);
      ReflectionTestUtils.setField(tidbConnector, "tidbCdcService", tidbCdcService);
      AtomicBoolean expectedData = new AtomicBoolean(false);
      ReflectionTestUtils.invokeMethod(tidbConnector, "onStop", connectorContext);
      AtomicBoolean actualData = (AtomicBoolean) ReflectionTestUtils.getField(tidbCdcService, "started");
      Assert.assertEquals(expectedData.get(), actualData.get());
   }

   @Test
   public void testReadBinlog() throws Throwable {
      TidbConfig tidbConfig = new TidbConfig();
      String database = "test1";
      String tableName = "test";
      tidbConfig.setPdServer("127.0.0.1:2379");
      AtomicBoolean started = new AtomicBoolean(false);
      tidbConfig.setDatabase(database);
      TidbCdcService tidbCdcService = new TidbCdcService(tidbConfig, new TapLog(), started);
      TapConnectorContext connectorContext = new TapConnectorContext(new TapNodeSpecification(),
              new DataMap(), new DataMap(), new TapLog());
      String id = "123456";
      ReflectionTestUtils.setField(connectorContext, "id", id);
      List<String> tableList = new ArrayList<>();
      tableList.add("test");
      StreamReadConsumer consumer = new StreamReadConsumer();
      try (
              MockedStatic<TiSession> tiSessionMockedStatic = mockStatic(TiSession.class);
      ) {
         TiSession tiSession = Mockito.mock(TiSession.class);
         tiSessionMockedStatic.when(() -> TiSession.create(Mockito.any())).thenReturn(tiSession);
         TiTableInfo tableInfo = Mockito.mock(TiTableInfo.class);
         Catalog catalog = Mockito.mock(Catalog.class);
         when(tiSession.getCatalog()).thenReturn(catalog);
         when(tiSession.getCatalog().getTable(database, tableName)).thenReturn(tableInfo);
         tidbCdcService.readBinlog(connectorContext, tableList, 12345678L, consumer);
         Map<String, JobClient> streamExecutionEnvironment = (Map<String, JobClient>) ReflectionTestUtils.getField(tidbCdcService, "streamExecutionEnvironment");
         Assert.assertTrue(MapUtils.isNotEmpty(streamExecutionEnvironment));
         Set<Map.Entry<String, JobClient>> entries = streamExecutionEnvironment.entrySet();
         for (Map.Entry<String, JobClient> entry : entries) {
            entry.getValue().cancel();
         }
         streamExecutionEnvironment.clear();
      }

   }
   @Test
   public void testOnStart() {
      TidbConnector tidbConnector = new TidbConnector();
      TidbConfig tidbConfig = new TidbConfig();
      AtomicBoolean started = new AtomicBoolean(true);
      TidbCdcService tidbCdcService = new TidbCdcService(tidbConfig, new TapLog(), started);
      DataMap connectionConfig = new DataMap();
      connectionConfig.put("username","root");
      connectionConfig.put("timezone","+8:00");
      TapConnectorContext connectorContext = new TapConnectorContext(new TapNodeSpecification(),
              connectionConfig,new DataMap(),new TapLog());
      ReflectionTestUtils.setField(tidbConnector, "tidbCdcService", tidbCdcService);
      AtomicBoolean expectedData = new AtomicBoolean(true);
      ReflectionTestUtils.invokeMethod(tidbConnector, "onStart", connectorContext);
      AtomicBoolean actualData = (AtomicBoolean) ReflectionTestUtils.getField(tidbConnector, "started");
      Assert.assertEquals(expectedData.get(), actualData.get());
   }


   @Test
   public void testRegisterCapabilities() {
      TidbConnector tidbConnector = new TidbConnector();
      ConnectorFunctions connectorFunctions = new ConnectorFunctions();
      TapCodecsRegistry codecRegistry = new TapCodecsRegistry();
      tidbConnector.registerCapabilities(connectorFunctions, codecRegistry);
      boolean actualData = connectorFunctions.getStreamReadFunction() != null
              && connectorFunctions.getTimestampToStreamOffsetFunction() != null;
      Assert.assertTrue(actualData);
   }


   @Test
   public void testStartStream() {
      StreamData streamData = new StreamData();
      String database = "test1";
      String tableName = "test";
      TiConfiguration tiConf = new TiConfiguration();

      TiKVChangeEventDeserializationSchema tiKVChangeEventDeserializationSchema
              = Mockito.mock(TiKVChangeEventDeserializationSchema.class);
      JobClient jobClient = streamData.startStream(database, tableName, tiConf, 178273847484L, tiKVChangeEventDeserializationSchema, new TapLog());
      Assert.assertTrue(jobClient != null);
      jobClient.cancel();
   }


   @Test
   public void testStartStreamException() {
      StreamData streamData = new StreamData();
      TiConfiguration tiConf = new TiConfiguration();

      TiKVChangeEventDeserializationSchema tiKVChangeEventDeserializationSchema
              = Mockito.mock(TiKVChangeEventDeserializationSchema.class);
      JobClient jobClient = streamData.startStream(null, null, tiConf, 178273847484L, null, new TapLog());
      Assert.assertTrue(jobClient == null);

   }

   @Test
   public void testHandleUpdateRowEvent() {

      TidbStreamEvent tidbStreamEvent = handleRowEvent("PUT");
      TapEvent tapEvent = tidbStreamEvent.getTapEvent();
      Assert.assertTrue(tapEvent instanceof TapUpdateRecordEvent);
      Assert.assertEquals(1, ((TapUpdateRecordEvent) tapEvent).getAfter().get("id"));

   }


   @Test
   public void testHandleDeleteRowEvent() {
      TidbStreamEvent tidbStreamEvent = handleRowEvent("DELETE");
      TapEvent tapEvent = tidbStreamEvent.getTapEvent();
      Assert.assertTrue(tapEvent instanceof TapDeleteRecordEvent);
      Assert.assertEquals(1, ((TapDeleteRecordEvent) tapEvent).getBefore().get("id"));

   }


   public  TidbStreamEvent handleRowEvent(String opType){
      String database = "test1";
      String tableName = "test";
      TiConfiguration tiConf = new TiConfiguration();
      String tapContextId = "12345678";
      LinkedBlockingQueue<TidbStreamEvent> logQueue = new LinkedBlockingQueue<>(5000);
      Map<String, LinkedBlockingQueue> logMap = new ConcurrentHashMap();
      logMap.put(database+tapContextId,logQueue);
      try (
              MockedStatic<TiSession> tiSessionMockedStatic = mockStatic(TiSession.class);
              MockedStatic<RowKey> rowKeyMockedStatic = mockStatic(RowKey.class);
              MockedStatic<TableCodec> tableCodecMockedStatic = mockStatic(TableCodec.class);


      ) {
         TiSession tiSession = Mockito.mock(TiSession.class);
         tiSessionMockedStatic.when(() ->TiSession.create(tiConf)).thenReturn(tiSession);
         TiTableInfo tableInfo = Mockito.mock(TiTableInfo.class);
         Catalog catalog = Mockito.mock(Catalog.class);
         when(tiSession.getCatalog()).thenReturn(catalog);
         when(tiSession.getCatalog().getTable(database, tableName)).thenReturn(tableInfo);
         TiKVChangeEventDeserializationSchemaImpl tiKVChangeEventDeserializationSchema =
                 new TiKVChangeEventDeserializationSchemaImpl(database, tableName, tapContextId, tiConf, logMap);
         Cdcpb.Event.Row row = Mockito.mock(Cdcpb.Event.Row.class);
         org.tikv.kvproto.Cdcpb.Event.Row.OpType result = org.tikv.kvproto.Cdcpb.Event.Row.OpType.valueOf(opType);
         when(row.getOpType()).thenReturn(result);
         when(row.getKey()).thenReturn(ByteString.copyFromUtf8("1"));
         RowKey rowKey = Mockito.mock(RowKey.class);
         int id = 1;
         int year = 8;
         Object[] tikvValues = new Object[]{id, year};
         rowKeyMockedStatic.when(() -> RowKey.decode(ByteString.copyFromUtf8("1").toByteArray())).thenReturn(rowKey);
         tableCodecMockedStatic.when(() -> decodeObjects(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(tikvValues);
         when(row.getValue()).thenReturn(ByteString.copyFromUtf8("1"));
         when(row.getOldValue()).thenReturn(ByteString.copyFromUtf8("2"));
         List<TiColumnInfo> columnInfos = new ArrayList<>();
         TiColumnInfo tiColumnInfo = new TiColumnInfo(1, "id", 2, IntegerType.ROW_ID_TYPE, true);
         TiColumnInfo tiColumnInfo1 = new TiColumnInfo(2, "year", 3, IntegerType.ROW_ID_TYPE, false);
         columnInfos.add(tiColumnInfo);
         columnInfos.add(tiColumnInfo1);
         when(tableInfo.getColumns()).thenReturn(columnInfos);
         tiKVChangeEventDeserializationSchema.handleRowEvent(row);
         TidbStreamEvent tidbStreamEvent = logQueue.poll();
          return tidbStreamEvent;
      } catch (InterruptedException ex) {
         throw new RuntimeException(ex);
      } catch (Exception e) {
         throw new RuntimeException(e);
      }

   }

   @Test
   public void testConvert2Map() throws Exception {
      String database = "test1";
      String tableName = "test";
      TiConfiguration tiConf = new TiConfiguration();
      Map<String, LinkedBlockingQueue> logMap = new ConcurrentHashMap();
      try (
              MockedStatic<TiSession> tiSessionMockedStatic = mockStatic(TiSession.class);

      ){
         TiSession tiSession = Mockito.mock(TiSession.class);
         tiSessionMockedStatic.when(() ->TiSession.create(tiConf)).thenReturn(tiSession);
         TiTableInfo tableInfo = Mockito.mock(TiTableInfo.class);
         Catalog catalog = Mockito.mock(Catalog.class);
         when(tiSession.getCatalog()).thenReturn(catalog);
         when(tiSession.getCatalog().getTable(database, tableName)).thenReturn(tableInfo);
         TiKVChangeEventDeserializationSchemaImpl tiKVChangeEventDeserializationSchema =
                 new TiKVChangeEventDeserializationSchemaImpl(database, tableName, "12345678", tiConf, logMap);
         int id = 1;
         int year =8;
         Object[] tikvValues = new Object[]{id,year};
         List<TiColumnInfo> columnInfos = new ArrayList<>();
         TiColumnInfo tiColumnInfo = new TiColumnInfo(1, "id", 2, IntegerType.ROW_ID_TYPE, true);
         TiColumnInfo tiColumnInfo1 = new TiColumnInfo(2, "year", 3, IntegerType.ROW_ID_TYPE, false);
         columnInfos.add(tiColumnInfo);
         columnInfos.add(tiColumnInfo1);
         when(tableInfo.getColumns()).thenReturn(columnInfos);
         Map<String, Object> actualData =ReflectionTestUtils.invokeMethod(tiKVChangeEventDeserializationSchema, "convert2Map", tableInfo,tikvValues);
         Assert.assertEquals(id,actualData.get("id"));
         Assert.assertEquals(year,actualData.get("year"));

      }

   }


}
