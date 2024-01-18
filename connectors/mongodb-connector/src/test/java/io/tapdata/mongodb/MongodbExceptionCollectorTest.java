package io.tapdata.mongodb;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.WriteConcernError;
import io.tapdata.exception.runtime.TapPdkSkippableDataEx;
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
    }
}
