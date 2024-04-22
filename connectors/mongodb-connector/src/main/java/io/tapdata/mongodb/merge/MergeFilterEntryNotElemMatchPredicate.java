package io.tapdata.mongodb.merge;

import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2024-04-22 20:25
 **/
public class MergeFilterEntryNotElemMatchPredicate implements MergeFilterEntryPredicate {

	@Override
	public boolean test(Map.Entry<String, Object> entry) {
		if (null == entry) {
			return false;
		}
		Object value = entry.getValue();
		if (value instanceof Map && ((Map) value).size() == 1 && ((Map) value).containsKey("$not")) {
			Object notMap = ((Map<?, ?>) value).get("$not");
			return !(notMap instanceof Map) || !((Map) notMap).containsKey("$elemMatch");
		}
		return true;
	}
}
