package zhgio.myss.elements;

import java.util.LinkedHashSet;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

@Data
@AllArgsConstructor
@ToString(exclude = { "tableName", "isAscending" })
public class Index {
	private String tableName;
	private String indexName;
	private LinkedHashSet<String> columnReferences; // needs to preserve the insertion order so we can identify ordinal position of every column reference
	private boolean isAscending;
}