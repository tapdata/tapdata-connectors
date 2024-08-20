package io.tapdata.mongodb.writer.error;

import com.mongodb.client.model.WriteModel;
import org.bson.Document;

import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2024-07-10 14:44
 **/
public class TapMongoBulkWriteException extends RuntimeException {
	private final List<WriteModel<Document>> writeModels;

	public TapMongoBulkWriteException(Throwable cause, List<WriteModel<Document>> writeModels) {
		super(cause);
		this.writeModels = writeModels;
	}

	@Override
	public String getMessage() {
		StringBuilder sb = new StringBuilder();
		sb.append("Bulk write failed with ")
				.append(null == writeModels ? 0 : writeModels.size())
				.append(":").append(System.lineSeparator());
		if (null != writeModels) {
			for (WriteModel<Document> writeModel : writeModels) {
				sb.append(writeModel.toString()).append(System.lineSeparator());
			}
		}
		return sb.toString();
	}
}
