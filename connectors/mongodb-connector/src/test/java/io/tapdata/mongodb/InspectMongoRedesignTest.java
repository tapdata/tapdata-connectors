package io.tapdata.mongodb;

import com.mongodb.client.*;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class InspectMongoRedesignTest {
    @Test void emptyPipelineRequiresInspectOptInAndCancelNeverClosesSharedClient() {
        var function=new MongodbExecuteCommandFunction();
        MongoClient client=mock(MongoClient.class);MongoDatabase db=mock(MongoDatabase.class);
        MongoCollection<Document> collection=mock(MongoCollection.class);
        AggregateIterable<Document> iterable=mock(AggregateIterable.class);MongoCursor<Document> cursor=mock(MongoCursor.class);
        when(client.getDatabase("db")).thenReturn(db);when(db.getCollection("rows")).thenReturn(collection);
        when(collection.aggregate(anyList())).thenReturn(iterable);when(iterable.allowDiskUse(true)).thenReturn(iterable);
        when(iterable.iterator()).thenReturn(cursor);when(cursor.hasNext()).thenReturn(false);
        Map<String,Object> params=new HashMap<>();params.put("database","db");params.put("collection","rows");params.put("pipeline",List.of());
        assertThrows(RuntimeException.class,()->function.aggregate(params,client,rows->{},()->true));
        verifyNoInteractions(client);
        params.put("inspectAllowEmptyPipeline",true);params.put("inspectOwnClient",true);params.put("queryTimeoutSeconds",2);
        List<Runnable> cancellations=new ArrayList<>();params.put("inspectRegisterCancel",(Consumer<Runnable>)cancellations::add);
        function.aggregate(params,client,rows->fail("Empty cursor"),()->true);
        assertEquals(1,cancellations.size());cancellations.forEach(Runnable::run);
        verify(cursor,atLeastOnce()).close();verify(client,never()).close();verify(iterable).maxTime(2,TimeUnit.SECONDS);
        // An ordinary caller still rejects empty pipelines, including the non-streaming overload.
        params.remove("inspectAllowEmptyPipeline");assertThrows(RuntimeException.class,()->function.aggregate(params,client));
    }
}
