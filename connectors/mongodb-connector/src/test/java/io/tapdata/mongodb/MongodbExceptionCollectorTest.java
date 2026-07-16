package io.tapdata.mongodb;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.WriteConcernError;
import io.tapdata.exception.TapPdkViolateUniqueEx;
import io.tapdata.exception.runtime.TapPdkSkippableDataEx;
import io.tapdata.mongodb.writer.error.TapMongoBulkWriteException;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/11/29 12:08 Create
 */
public class MongodbExceptionCollectorTest {

    @Test
    void testWriteErrorIndexWithinDataSize() {
        Object firstEvent = new Object();
        List<Object> dataList = new ArrayList<>();
        dataList.add(firstEvent);
        dataList.add(new Object());

        TapPdkViolateUniqueEx exception = Assertions.assertThrows(
                TapPdkViolateUniqueEx.class,
                () -> new MongodbExceptionCollector().collectViolateUnique(dataList, mongoBulkWriteException(11000, 0))
        );

        Assertions.assertSame(firstEvent, exception.getData());
    }

    @Test
    void testWriteErrorIndexEqualsDataSize() {
        Object lastEvent = new Object();
        List<Object> dataList = new ArrayList<>();
        dataList.add(new Object());
        dataList.add(lastEvent);

        TapPdkViolateUniqueEx exception = Assertions.assertThrows(
                TapPdkViolateUniqueEx.class,
                () -> new MongodbExceptionCollector().collectViolateUnique(dataList, mongoBulkWriteException(11000, dataList.size()))
        );

        Assertions.assertSame(lastEvent, exception.getData());
    }

    @Test
    void testEmptyDataList() {
        List<Object> dataList = new ArrayList<>();

        TapPdkViolateUniqueEx exception = Assertions.assertThrows(
                TapPdkViolateUniqueEx.class,
                () -> new MongodbExceptionCollector().collectViolateUnique(dataList, mongoBulkWriteException(11000, 0))
        );

        Assertions.assertSame(dataList, exception.getData());
    }

    private MongoBulkWriteException mongoBulkWriteException(int code, int index) {
        List<BulkWriteError> writeErrors = new ArrayList<>();
        writeErrors.add(new BulkWriteError(code, "duplicate key error", Mockito.mock(BsonDocument.class), index));
        return new MongoBulkWriteException(
                Mockito.mock(BulkWriteResult.class),
                writeErrors,
                Mockito.mock(WriteConcernError.class),
                Mockito.mock(ServerAddress.class),
                new HashSet<>()
        );
    }

    @Nested
    class ExCode_10010 {
        @Test
        void testCannotUseThePart() {
            // mock exception
            List<Object> dataList = new ArrayList<>();
            BulkWriteResult writeResult = Mockito.mock(BulkWriteResult.class);
            List<BulkWriteError> writeErrors = new ArrayList<>();
            WriteConcernError writeConcernError = Mockito.mock(WriteConcernError.class);
            ServerAddress serverAddress = Mockito.mock(ServerAddress.class);
            Set<String> errorLabels = new HashSet<>();

            MongodbExceptionCollector mongodbExceptionCollector = new MongodbExceptionCollector();
            MongoBulkWriteException mongoBulkWriteException = new MongoBulkWriteException(writeResult, writeErrors, writeConcernError, serverAddress, errorLabels);

            // no match exception
            writeErrors.add(new BulkWriteError(-3, "test", Mockito.mock(BsonDocument.class), 0));
            Assertions.assertDoesNotThrow(() -> mongodbExceptionCollector.throwWriteExIfNeed(dataList, mongoBulkWriteException));

            // match TapPdkTraverseElementEx exception
            writeErrors.add(new BulkWriteError(-3, "cannot use the part (subDoc of subDoc.a) to traverse the element ({subDoc: \"xxx\"})", Mockito.mock(BsonDocument.class), 0));
            Assertions.assertThrows(TapPdkSkippableDataEx.class, () -> mongodbExceptionCollector.throwWriteExIfNeed(dataList, mongoBulkWriteException));
        }

        @Test
        void testCannotUseThePart_TapMongoBulkWriteException() {
            // mock exception
            List<Object> dataList = new ArrayList<>();
            BulkWriteResult writeResult = Mockito.mock(BulkWriteResult.class);
            List<BulkWriteError> writeErrors = new ArrayList<>();
            WriteConcernError writeConcernError = Mockito.mock(WriteConcernError.class);
            ServerAddress serverAddress = Mockito.mock(ServerAddress.class);
            Set<String> errorLabels = new HashSet<>();

            MongodbExceptionCollector mongodbExceptionCollector = new MongodbExceptionCollector();
            MongoBulkWriteException mongoBulkWriteException = new MongoBulkWriteException(writeResult, writeErrors, writeConcernError, serverAddress, errorLabels);
            TapMongoBulkWriteException tapMongoBulkWriteException = new TapMongoBulkWriteException(mongoBulkWriteException,new ArrayList<>());

            // no match exception
            writeErrors.add(new BulkWriteError(-3, "test", Mockito.mock(BsonDocument.class), 0));
            Assertions.assertDoesNotThrow(() -> mongodbExceptionCollector.throwWriteExIfNeed(dataList, tapMongoBulkWriteException));

            // match TapPdkTraverseElementEx exception
            writeErrors.add(new BulkWriteError(-3, "cannot use the part (subDoc of subDoc.a) to traverse the element ({subDoc: \"xxx\"})", Mockito.mock(BsonDocument.class), 0));
            Assertions.assertThrows(TapPdkSkippableDataEx.class, () -> mongodbExceptionCollector.throwWriteExIfNeed(dataList, tapMongoBulkWriteException));
        }
    }
}
