package io.tapdata.mongodb.writer.error;

import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;

/**
 * Marker class for indicating that a write operation should be ignored (no retry, no error).
 * Uses a special singleton InsertOneModel instance as a marker.
 */
public final class IgnoreWriteModel {
	/**
	 * Special marker instance to indicate an operation should be ignored.
	 * This is a singleton InsertOneModel with a special marker document.
	 */
	public static final WriteModel<Document> INSTANCE = new InsertOneModel<>(new Document("__IGNORE_MARKER__", true));

	private IgnoreWriteModel() {
		// Prevent instantiation
	}
}

