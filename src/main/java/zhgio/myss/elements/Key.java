package zhgio.myss.elements;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@AllArgsConstructor
@RequiredArgsConstructor
public class Key {

	private final boolean isPrimary;
	private final String tableName;
	private final String columnName;

	private String fkName;
	private String fkColumnName;

}
