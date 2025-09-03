package io.tapdata.js.connector.server.function.support;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.js.connector.server.function.JSFunctionNames;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.ConnectorCapabilities;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.mockito.Mockito.*;

class JSWriteRecordFunctionTest {
    JSWriteRecordFunction function;

    @BeforeEach
    void setUp() {
        function = mock(JSWriteRecordFunction.class);
    }

    @Nested
    class DoSubFunctionNotSupportedTest {
        @BeforeEach
        void init() {
            when(function.doSubFunctionNotSupported()).thenCallRealMethod();
        }

        @Test
        void testAll() {
            when(function.doNotSupport(JSFunctionNames.InsertRecordFunction)).thenReturn(true);
            when(function.doNotSupport(JSFunctionNames.DeleteRecordFunction)).thenReturn(true);
            when(function.doNotSupport(JSFunctionNames.UpdateRecordFunction)).thenReturn(true);
            when(function.doNotSupport(JSFunctionNames.InsertRecordBatchFunction)).thenReturn(true);
            when(function.doNotSupport(JSFunctionNames.UpdateRecordBatchFunction)).thenReturn(true);
            when(function.doNotSupport(JSFunctionNames.DeleteRecordBatchFunction)).thenReturn(true);
            boolean supported = function.doSubFunctionNotSupported();
            Assertions.assertTrue(supported);
        }

        @Test
        void testInsertRecordFunction() {
            when(function.doNotSupport(JSFunctionNames.InsertRecordFunction)).thenReturn(true);
            when(function.doNotSupport(JSFunctionNames.DeleteRecordFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.UpdateRecordFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.InsertRecordBatchFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.UpdateRecordBatchFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.DeleteRecordBatchFunction)).thenReturn(false);

            boolean supported = function.doSubFunctionNotSupported();
            Assertions.assertFalse(supported);
        }

        @Test
        void testDeleteRecordFunction() {
            when(function.doNotSupport(JSFunctionNames.InsertRecordFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.DeleteRecordFunction)).thenReturn(true);
            when(function.doNotSupport(JSFunctionNames.UpdateRecordFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.InsertRecordBatchFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.UpdateRecordBatchFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.DeleteRecordBatchFunction)).thenReturn(false);

            boolean supported = function.doSubFunctionNotSupported();
            Assertions.assertFalse(supported);
        }

        @Test
        void testUpdateRecordFunction() {
            when(function.doNotSupport(JSFunctionNames.InsertRecordFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.DeleteRecordFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.UpdateRecordFunction)).thenReturn(true);
            when(function.doNotSupport(JSFunctionNames.InsertRecordBatchFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.UpdateRecordBatchFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.DeleteRecordBatchFunction)).thenReturn(false);

            boolean supported = function.doSubFunctionNotSupported();
            Assertions.assertFalse(supported);
        }

        @Test
        void testInsertRecordBatchFunction() {
            when(function.doNotSupport(JSFunctionNames.InsertRecordFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.DeleteRecordFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.UpdateRecordFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.InsertRecordBatchFunction)).thenReturn(true);
            when(function.doNotSupport(JSFunctionNames.UpdateRecordBatchFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.DeleteRecordBatchFunction)).thenReturn(false);

            boolean supported = function.doSubFunctionNotSupported();
            Assertions.assertFalse(supported);
        }

        @Test
        void testUpdateRecordBatchFunction() {
            when(function.doNotSupport(JSFunctionNames.InsertRecordFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.DeleteRecordFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.UpdateRecordFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.InsertRecordBatchFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.UpdateRecordBatchFunction)).thenReturn(true);
            when(function.doNotSupport(JSFunctionNames.DeleteRecordBatchFunction)).thenReturn(false);

            boolean supported = function.doSubFunctionNotSupported();
            Assertions.assertFalse(supported);
        }

        @Test
        void testInsertDeleteRecordBatchFunction() {
            when(function.doNotSupport(JSFunctionNames.InsertRecordFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.DeleteRecordFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.UpdateRecordFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.InsertRecordBatchFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.UpdateRecordBatchFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.DeleteRecordBatchFunction)).thenReturn(true);

            boolean supported = function.doSubFunctionNotSupported();
            Assertions.assertFalse(supported);
        }
        @Test
        void testDeleteRecordBatchFunction() {
            when(function.doNotSupport(JSFunctionNames.InsertRecordFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.DeleteRecordFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.UpdateRecordFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.InsertRecordBatchFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.UpdateRecordBatchFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.DeleteRecordBatchFunction)).thenReturn(true);

            boolean supported = function.doSubFunctionNotSupported();
            Assertions.assertFalse(supported);
        }

        @Test
        void testNoneSupported() {
            when(function.doNotSupport(JSFunctionNames.InsertRecordFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.DeleteRecordFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.UpdateRecordFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.InsertRecordBatchFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.UpdateRecordBatchFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.DeleteRecordBatchFunction)).thenReturn(false);

            boolean supported = function.doSubFunctionNotSupported();
            Assertions.assertFalse(supported);
        }

        @Test
        void testMultipleFunctionsNotSupported() {
            when(function.doNotSupport(JSFunctionNames.InsertRecordFunction)).thenReturn(true);
            when(function.doNotSupport(JSFunctionNames.DeleteRecordFunction)).thenReturn(true);
            when(function.doNotSupport(JSFunctionNames.UpdateRecordFunction)).thenReturn(true);
            when(function.doNotSupport(JSFunctionNames.InsertRecordBatchFunction)).thenReturn(true);
            when(function.doNotSupport(JSFunctionNames.UpdateRecordBatchFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.DeleteRecordBatchFunction)).thenReturn(false);

            boolean supported = function.doSubFunctionNotSupported();
            Assertions.assertFalse(supported);
        }

        @Test
        void testBatchFunctionsNotSupported() {
            when(function.doNotSupport(JSFunctionNames.InsertRecordFunction)).thenReturn(true);
            when(function.doNotSupport(JSFunctionNames.DeleteRecordFunction)).thenReturn(true);
            when(function.doNotSupport(JSFunctionNames.UpdateRecordFunction)).thenReturn(true);
            when(function.doNotSupport(JSFunctionNames.InsertRecordBatchFunction)).thenReturn(true);
            when(function.doNotSupport(JSFunctionNames.UpdateRecordBatchFunction)).thenReturn(true);
            when(function.doNotSupport(JSFunctionNames.DeleteRecordBatchFunction)).thenReturn(false);

            boolean supported = function.doSubFunctionNotSupported();
            Assertions.assertFalse(supported);
        }
    }

    @Nested
    class SupportButchTest {

        @BeforeEach
        void init() {
            when(function.supportButch("i")).thenCallRealMethod();
            when(function.supportButch("u")).thenCallRealMethod();
            when(function.supportButch("d")).thenCallRealMethod();
        }


        @Test
        void testInsert() {
            when(function.doNotSupport(JSFunctionNames.InsertRecordBatchFunction)).thenReturn(true);
            when(function.doNotSupport(JSFunctionNames.InsertRecordFunction)).thenReturn(false);
            JSFunctionNames support = function.supportButch("i");
            Assertions.assertEquals(JSFunctionNames.InsertRecordFunction, support);
        }

        @Test
        void testInsert1() {
            when(function.doNotSupport(JSFunctionNames.InsertRecordBatchFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.InsertRecordFunction)).thenReturn(true);
            JSFunctionNames support = function.supportButch("i");
            Assertions.assertEquals(JSFunctionNames.InsertRecordBatchFunction, support);
        }

        @Test
        void testUpdate() {
            when(function.doNotSupport(JSFunctionNames.UpdateRecordBatchFunction)).thenReturn(true);
            when(function.doNotSupport(JSFunctionNames.UpdateRecordFunction)).thenReturn(false);
            JSFunctionNames support = function.supportButch("u");
            Assertions.assertEquals(JSFunctionNames.UpdateRecordFunction, support);
        }

        @Test
        void testUpdate1() {
            when(function.doNotSupport(JSFunctionNames.UpdateRecordBatchFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.UpdateRecordFunction)).thenReturn(true);
            JSFunctionNames support = function.supportButch("u");
            Assertions.assertEquals(JSFunctionNames.UpdateRecordBatchFunction, support);
        }

        @Test
        void testDelete() {
            when(function.doNotSupport(JSFunctionNames.DeleteRecordBatchFunction)).thenReturn(true);
            when(function.doNotSupport(JSFunctionNames.DeleteRecordFunction)).thenReturn(false);
            JSFunctionNames support = function.supportButch("d");
            Assertions.assertEquals(JSFunctionNames.DeleteRecordFunction, support);
        }
        @Test
        void testDelete1() {
            when(function.doNotSupport(JSFunctionNames.DeleteRecordBatchFunction)).thenReturn(false);
            when(function.doNotSupport(JSFunctionNames.DeleteRecordFunction)).thenReturn(true);
            JSFunctionNames support = function.supportButch("d");
            Assertions.assertEquals(JSFunctionNames.DeleteRecordBatchFunction, support);
        }
    }

    @Nested
    class ExecDropTest {
        TapConnectorContext context;
        List<Map<String, Object>> execData;
        Consumer<WriteListResult<TapRecordEvent>> consumer;
        @BeforeEach
        void init() {
            context = mock(TapConnectorContext.class);
            execData = new ArrayList<>();
            execData.add(new HashMap<>());
            consumer = mock(Consumer.class);
            doCallRealMethod().when(function).execDrop("", context, execData, consumer, "");
        }
        @Test
        void testInsertRecordBatchFunction() {
            when(function.supportButch("")).thenReturn(JSFunctionNames.InsertRecordBatchFunction);
            doNothing().when(function).exec(context, execData, JSFunctionNames.InsertRecordBatchFunction, consumer, "");
            function.execDrop("", context, execData, consumer, "");
            verify(function).exec(context, execData, JSFunctionNames.InsertRecordBatchFunction, consumer, "");
            verify(function).supportButch("");
        }
        @Test
        void testInsertRecordFunction() {
            doNothing().when(function).exec(context, execData, JSFunctionNames.InsertRecordFunction, consumer, "");
            when(function.supportButch("")).thenReturn(JSFunctionNames.InsertRecordFunction);
            function.execDrop("", context, execData, consumer, "");
            verify(function).exec(context, execData.get(0), JSFunctionNames.InsertRecordFunction, consumer, "");
            verify(function).supportButch("");
        }
    }

    @Nested
    class CapabilitiesTest {
        TapConnectorContext context;
        ConnectorCapabilities capabilities;

        @BeforeEach
        void init() {
            capabilities = mock(ConnectorCapabilities.class);
            context = mock(TapConnectorContext.class);
            when(function.capabilities(context)).thenCallRealMethod();
        }

        @Test
        void testNull() {
            when(context.getConnectorCapabilities()).thenReturn(null);
            Map<String, String> capabilities1 = function.capabilities(context);
            Assertions.assertNotNull(capabilities1);
            Assertions.assertEquals(2, capabilities1.size());
            Assertions.assertEquals(ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS, capabilities1.get(ConnectionOptions.DML_INSERT_POLICY));
            Assertions.assertEquals(ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS, capabilities1.get(ConnectionOptions.DML_UPDATE_POLICY));
        }

        @Test
        void testNotNull() {
            when(context.getConnectorCapabilities()).thenReturn(capabilities);
            when(capabilities.getCapabilityAlternativeMap()).thenReturn(new HashMap<>());
            Map<String, String> capabilities1 = function.capabilities(context);
            Assertions.assertNotNull(capabilities1);
            Assertions.assertEquals(2, capabilities1.size());
            Assertions.assertEquals(ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS, capabilities1.get(ConnectionOptions.DML_INSERT_POLICY));
            Assertions.assertEquals(ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS, capabilities1.get(ConnectionOptions.DML_UPDATE_POLICY));
        }
    }
}