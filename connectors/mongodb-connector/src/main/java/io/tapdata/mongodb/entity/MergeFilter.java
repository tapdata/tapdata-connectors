package io.tapdata.mongodb.entity;

import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2024-04-19 18:42
 **/
public class MergeFilter {
	private final List<Document> filters;

	private final boolean append;

	public MergeFilter(boolean append) {
		this.filters = new ArrayList<>();
		this.append = append;
	}

	public void addFilter(Document filter) {
		filters.add(filter);
	}

	public void removeLast() {
		if (!filters.isEmpty()) {
			filters.remove(filters.size() - 1);
		}
	}

	public Document appendFilters() {
		if (!filters.isEmpty()) {
			Document document = new Document();
			for (Document filter : filters) {
				document.putAll(filter);
			}
			return document;
		}
		return null;
	}

	public List<Document> getFilters() {
		return filters;
	}

	public boolean isAppend() {
		return append;
	}
}
