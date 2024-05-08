package io.tapdata.mongodb.merge;

import java.util.Map;
import java.util.function.Predicate;

/**
 * @author samuel
 * @Description
 * @create 2024-04-22 20:21
 **/
public interface MergeFilterEntryPredicate extends Predicate<Map.Entry<String, Object>> {
}
