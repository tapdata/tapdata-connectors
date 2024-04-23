package io.tapdata.mongodb.merge;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2024-04-22 20:28
 **/
public class MergeFilterManager {
	private static final List<MergeFilterEntryPredicate> mergeFilterEntryPredicateList;

	static {
		mergeFilterEntryPredicateList = new ArrayList<>();
		mergeFilterEntryPredicateList.add(new MergeFilterEntryNotElemMatchPredicate());
	}

	private MergeFilterManager() {
	}

	public static boolean test(Map.Entry<String, Object> entry) {
		if (null == entry) {
			return true;
		}
		for (MergeFilterEntryPredicate mergeFilterEntryPredicate : mergeFilterEntryPredicateList) {
			if (!mergeFilterEntryPredicate.test(entry)) {
				return true;
			}
		}
		return false;
	}
}
