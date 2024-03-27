package io.tapdata.connector.mysql;

import io.tapdata.common.ddl.DDLFactory;
import io.tapdata.connector.mysql.entity.MysqlStreamEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.logger.Log;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MysqlReaderTest {

    @Nested
    class wrapDDL{
        MysqlReader mysqlReader = new MysqlReader(mock(MysqlJdbcContextV2.class),mock(Log.class),()->{return true;});
        @Test
        void test(){
            SourceRecord sourceRecord = mock(SourceRecord.class);
            Struct struct = mock(Struct.class);
            when(sourceRecord.value()).thenReturn(struct);
            when(struct.getStruct(anyString())).thenReturn(mock(Struct.class));
            when(struct.getString(anyString())).thenReturn("ALTER TABLE \"C##TAPDATA\".\"TT_DDL\" \n" +
                    "ADD (\"TT\" VARCHAR2(255));");
            List<MysqlStreamEvent> result = mysqlReader.wrapDDL(sourceRecord);
            TapDDLEvent resultEvent =(TapDDLEvent) result.get(0).getTapEvent();
            Assertions.assertEquals("ALTER TABLE \"C##TAPDATA\".\"TT_DDL\" \n" +
                    "ADD (\"TT\" VARCHAR2(255));",resultEvent.getOriginDDL());
        }
        @Test
        void testUnknownEvent(){
            SourceRecord sourceRecord = mock(SourceRecord.class);
            Struct struct = mock(Struct.class);
            when(sourceRecord.value()).thenReturn(struct);
            when(struct.getStruct(anyString())).thenReturn(mock(Struct.class));
            when(struct.getString(anyString())).thenReturn("Unknown DDL");
            try(MockedStatic<DDLFactory> ddlFactoryMockedStatic = Mockito.mockStatic(DDLFactory.class)){
                ddlFactoryMockedStatic.when(()->DDLFactory.ddlToTapDDLEvent(any(),any(),any(),any(),any())).thenThrow(new Throwable("ERROR"));
                List<MysqlStreamEvent> result = mysqlReader.wrapDDL(sourceRecord);
                TapDDLEvent resultEvent =(TapDDLEvent) result.get(0).getTapEvent();
                Assertions.assertEquals("Unknown DDL",resultEvent.getOriginDDL());
            }
        }

    }
}
