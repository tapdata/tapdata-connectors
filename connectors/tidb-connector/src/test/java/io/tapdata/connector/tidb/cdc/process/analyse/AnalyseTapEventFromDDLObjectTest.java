package io.tapdata.connector.tidb.cdc.process.analyse;


import io.tapdata.common.ddl.DDLFactory;
import io.tapdata.common.ddl.type.DDLParserType;
import io.tapdata.common.ddl.wrapper.DDLWrapperConfig;
import io.tapdata.connector.tidb.cdc.process.ddl.entity.DDLObject;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapRenameTableEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.pdk.apis.consumer.StreamReadOneByOneConsumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

class AnalyseTapEventFromDDLObjectTest {
    AnalyseTapEventFromDDLObject analyseTapEventFromDDLObject;
    KVReadOnlyMap<TapTable> tapTableMap;
    StreamReadOneByOneConsumer consumer;
    Map<String, Map<String, Long>> offset;

    @BeforeEach
    void init() {
        tapTableMap = mock(KVReadOnlyMap.class);
        consumer = mock(StreamReadOneByOneConsumer.class);
        offset = mock(Map.class);
        analyseTapEventFromDDLObject = new AnalyseTapEventFromDDLObject(tapTableMap).withConsumer(consumer, offset);
        doNothing().when(consumer).accept(any(), anyMap());
    }

    @Nested
    class AnalyseTest {
        DDLObject ddlObject;
        AnalyseColumnFilter<DDLObject> filter;
        Log log;
        TapEvent tapEvent;
        @BeforeEach
        void init() {
            tapEvent = new TapRenameTableEvent();
            ddlObject = new DDLObject();
            ddlObject.setQuery("SQL");
            filter = mock(AnalyseColumnFilter.class);
            log = mock(Log.class);
            doNothing().when(log).debug(anyString(), anyString());
        }

        @Test
        void testNormal() {
            try(MockedStatic<DDLFactory> df = mockStatic(DDLFactory.class)) {
                df.when(() -> DDLFactory.ddlToTapDDLEvent(
                        any(DDLParserType.class),
                        anyString(),
                        any(DDLWrapperConfig.class),
                        any(KVReadOnlyMap.class),
                        any(Consumer.class)
                )).thenAnswer(a -> {
                    Consumer consumer = a.getArgument(4, Consumer.class);
                    consumer.accept(tapEvent);
                    return null;
                });
                List<TapEvent> analyse = analyseTapEventFromDDLObject.analyse(ddlObject, filter, log);
                Assertions.assertNotNull(analyse);
            }
        }

        @Test
        void testException() {
            try(MockedStatic<DDLFactory> df = mockStatic(DDLFactory.class)) {
                df.when(() -> DDLFactory.ddlToTapDDLEvent(
                        any(DDLParserType.class),
                        anyString(),
                        any(DDLWrapperConfig.class),
                        any(KVReadOnlyMap.class),
                        any(Consumer.class)
                )).thenThrow(new Throwable("Failed"));
                Assertions.assertThrows(CoreException.class, () -> analyseTapEventFromDDLObject.analyse(ddlObject, filter, log));
            }
        }
    }
}