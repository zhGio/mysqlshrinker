package zhgio.myss.elements;

import java.math.BigDecimal;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import zhgio.myss.commons.DataType;
import zhgio.myss.contracts.StatementsWriter;

import static zhgio.myss.commons.Keyword.BACKTICK;
import static zhgio.myss.commons.Keyword.COMMA;
import static zhgio.myss.commons.Keyword.EMPTY_STR;
import static zhgio.myss.commons.Keyword.SPACE;

@Data
@ToString(exclude = { "schemaName", "columns", "numberOfRowsExact", "numberOfRowsApprox" })
@EqualsAndHashCode(exclude = "columns")
@Slf4j
public class Table implements StatementsWriter {

	private String schemaName;
	private String tableName;
	private Set<Column> columns;
	private BigDecimal tableSizeInMb;
	private long numberOfRowsExact;
	private long numberOfRowsApprox; // exact can take a a while to count while approx is off by 10-20% but gives you a general idea of row count

	// table PK column name
	private Set<Key> primaryKeys;

	// collection of FKs: key - referenced table name, value - foreign key column
	private Set<Key> foreignKeys;
	private Set<Index> indices;

	private String sqlStatement;

	public Table(String schemaName, String tableName) {
		this.schemaName = schemaName;
		this.tableName = tableName;
	}

	/**
	 * sets columns size, decimal precision, signed/unsigned, Extra column
	 * because this sort of data is missing in the databaseMetaData object or is inconsistent
	 */
	public void setTableDetailsAndExtras(List<Map<String, Object>> rows) {
		Map<String, Column> columnsAsMap = this.getColumnsAsMap();
		rows.forEach(rowMap -> columnsAsMap.get(rowMap.get("Field")).updateColumnFromTypeString(rowMap.get("Type")));
		rows.forEach(rowMap -> columnsAsMap.get(rowMap.get("Field")).updateColumnFromExtraString(rowMap.get("Extra")));
		log.info("Set type details for table {}", tableName);
	}

	/**
	 * Converts table and columns' data definition to a native SQL CREATE statement.
	 */
	@Override
	public void writeCreateStatement() {
		log.info("Starting to write the CREATE TABLE statement for table {}", this.getTableName());
		StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
		sb.append(BACKTICK).append(this.getTableName()).append(BACKTICK).append(SPACE).append("(").append(SPACE);

		/*
		 * appending columns
		 */
		//@formatter:off
		this.getColumns().forEach(
				column ->
						sb.append(BACKTICK).append(column.getColumnName()).append(BACKTICK).append(SPACE)
						.append(column.isBitOrBoolean() ? DataType.TINYINT : column.getType() == DataType.TIMESTAMP ? DataType.DATETIME : column.getType())
						.append(column.isBitOrBoolean() ? "(1) ": column.appendColumnDetails()).append(column.isUnsigned() ? " unsigned " : SPACE)
						.append(!column.isNullable() ? "NOT NULL " : EMPTY_STR)
						.append(column.isDefaultable() ? " DEFAULT " + column.getDefaultValue() : column.isNullable() ? " DEFAULT NULL " : EMPTY_STR)
						.append(column.isAutoincrement() ? "AUTO_INCREMENT" : EMPTY_STR)
						.append(column.getExtra() != null && !column.getExtra().isEmpty() ? SPACE + column.getExtra() : EMPTY_STR)
						.append(COMMA).append(SPACE)
		);
		//@formatter:on
		log.debug("Appended columns");

		/*
		 * append primary key(s)
		 */
		if (this.getPrimaryKeys() != null && !this.getPrimaryKeys().isEmpty()) {
			//@formatter:off
				sb.append("PRIMARY KEY (");
				this.getPrimaryKeys().forEach(
						primaryKey ->
									sb.append(BACKTICK).append(primaryKey.getColumnName()).append(BACKTICK).append(COMMA)

				);
				//@formatter:on
			sb.deleteCharAt(sb.length() - 1).append(")"); // delete last comma
		}
		log.debug("Appended primary keys");

		/*
		 * appending indices
		 */
		if (!this.getIndices().isEmpty()) {
			sb.append(COMMA).append(SPACE);
			//@formatter:off
			this.getIndices().forEach(
				index ->
						sb.append("KEY ").append(BACKTICK).append(index.getIndexName())
						.append(BACKTICK).append(SPACE).append("(")
						.append(listToString(index.getColumnReferences()))
						.append(")").append(COMMA).append(SPACE)
			);}
			//@formatter:on
		log.debug("Appended indices");

		StringBuilder newSb; // have to reassign the string builder because forEach complains about mutability
		newSb = findAndRemoveDanglingComma(sb);

		newSb.append(")");
		this.setSqlStatement(newSb.toString());
		log.info("Wrote the CREATE TABLE {} statement successfully", this.getTableName());
		log.debug(newSb.toString());
	}

	/**
	 * Builds an ALTER TABLE SQL statement which will bind the foreign key constraints of #this table to the referencing tables.
	 */
	@Override
	public void writeAlterTableAddFkConstraintsStatement() {
		log.info("Writing the ALTER TABLE statement for table {}", this.getTableName());

		StringBuilder sb = new StringBuilder("ALTER TABLE ");
		sb.append(this.getTableName());

		//@formatter:off
		this.getForeignKeys().forEach(
			fk ->
				 sb.append(" ADD CONSTRAINT ").append(BACKTICK)
				 .append(fk.getFkName()).append(BACKTICK).append(SPACE)
				 .append("FOREIGN KEY (").append(BACKTICK).append(fk.getFkColumnName()).append(BACKTICK).append(")")
				 .append(" REFERENCES ").append(BACKTICK).append(fk.getTableName()).append(BACKTICK)
				 .append(" (").append(BACKTICK).append(fk.getColumnName()).append(BACKTICK).append(") ").append(COMMA)
		);
		//@formatter:on
		sb.deleteCharAt(sb.length() - 1); // delete last comma
		log.info("Wrote the ALTER TABLE statement successfully");
		this.setSqlStatement(sb.toString());
	}

	// helper methods
	private String listToString(LinkedHashSet<String> columnReferences) {
		StringBuilder resultBuilder = new StringBuilder();
		columnReferences.forEach(colRef -> resultBuilder.append(BACKTICK).append(colRef).append(BACKTICK).append(COMMA));
		resultBuilder.deleteCharAt(resultBuilder.length() - 1);
		return resultBuilder.toString();
	}

	/**
	 * deletes some possible dangling commas in the string builder
	 * @param sb - query string builder
	 */
	private StringBuilder findAndRemoveDanglingComma(StringBuilder sb) {
		// check if comma is the last character
		// have to reassign the sb because trimming can only be called on string objects
		StringBuilder newSb = new StringBuilder(sb.toString().trim());
		char lastChar = newSb.charAt(newSb.length() - 1);
		if (String.valueOf(lastChar).equals(COMMA)) {
			return newSb.deleteCharAt(newSb.length() - 1);
		}
		return newSb;
	}

	/**
	 * @return the columns as key-value pairs where the key is the column name
	 */
	private Map<String, Column> getColumnsAsMap() {
		return this.getColumns().stream().collect(Collectors.toMap(Column::getColumnName, Function.identity()));
	}

}