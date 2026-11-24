package io.tapdata.mongodb.writer;

import io.tapdata.mongodb.entity.MergeBundle;
import io.tapdata.mongodb.entity.MergeResult;
import io.tapdata.pdk.apis.entity.merge.MergeLookupResult;
import io.tapdata.pdk.apis.entity.merge.MergeTableProperties;
import io.tapdata.mongodb.entity.MergeFilter;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.bson.Document;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class MongodbNullMergeFilterTest {
    @ParameterizedTest
    @ValueSource(strings = {"absent", "replace", "append"})
    void lookupInsertPreservesChildUpdateWithEachParentFilterMode(String mode) {
        MergeTableProperties parent = properties("");
        MergeTableProperties child = properties("child");
        Map<String, Object> source = new Document("id", 1);
        MergeBundle bundle = new MergeBundle(MergeBundle.EventOperation.INSERT, null, source);
        MergeLookupResult lookup = new MergeLookupResult();
        lookup.setProperty(child);
        lookup.setData(new Document("id", 2).append("value", "child-value"));
        lookup.setDataExists(true);
        List<MergeResult> results = new ArrayList<>();

        MergeFilter filter = "absent".equals(mode) ? null : new MergeFilter("append".equals(mode));
        assertDoesNotThrow(() -> MongodbMergeOperate.recursiveMerge(bundle, parent, results,
                Collections.singletonList(lookup), null, null, filter, 1, null));

        assertTrue(results.stream().anyMatch(result -> {
            Object set = result.getUpdate().get("$set");
            return set instanceof Map && "child-value".equals(((Map<?, ?>) set).get("child.value"));
        }));
    }

    private MergeTableProperties properties(String path) {
        MergeTableProperties properties = new MergeTableProperties();
        properties.setMergeType(MergeTableProperties.MergeType.updateWrite);
        properties.setTargetPath(path);
        Map<String, String> joinKey = new HashMap<>();
        joinKey.put("source", "id");
        joinKey.put("target", "id");
        properties.setJoinKeys(Collections.singletonList(joinKey));
        return properties;
    }
}
