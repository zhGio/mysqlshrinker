package zhgio.myss.elements;

import java.math.BigDecimal;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import zhgio.myss.commons.DataType;
import zhgio.myss.contracts.MetaDataSetter;
import zhgio.myss.contracts.MetaDataWriter;

import static zhgio.myss.commons.Keyword.BACKTICK;
import static zhgio.myss.commons.Keyword.COLUMN_NAME;
import static zhgio.myss.commons.Keyword.COMMA;
import static zhgio.myss.commons.Keyword.DATA_TYPE;
import static zhgio.myss.commons.Keyword.EMPTY_STR;
import static zhgio.myss.commons.Keyword.IS_AUTOINCREMENT;
import static zhgio.myss.commons.Keyword.IS_NULLABLE;
import static zhgio.myss.commons.Keyword.SPACE;
import static zhgio.myss.commons.Keyword.WILDCARD;
import static zhgio.myss.commons.Keyword.YES;
import static zhgio.myss.commons.Query.QUERY_SIZE_ONE_TABLE;

@Data
@ToString(exclude = { "schemaName", "columns", "numberOfRowsExact", "numberOfRowsApprox", "dataSource" })
@EqualsAndHashCode(exclude = "columns")
@Slf4j
public class Table implements MetaDataSetter, MetaDataWriter {

	private String schemaName;
	private String tableName;
	private Set<Column> columns;
	private BigDecimal tableSizeInMb;
	private long numberOfRowsExact;
	private long numberOfRowsApprox; // exact can take a a while to count while approx is off by 10-20% but gives you a general idea of row count

	private DataSource dataSource;
	private DatabaseMetaData databaseMetaData;

	// table PK column name
	private Set<Key> primaryKeys;

	// collection of FKs: key - referenced table name, value - foreign key column
	private Set<Key> foreignKeys;
	private Set<Index> indices;

	private String sqlStatement;

	public Table(String schemaName, String tableName, DataSource dataSource) throws SQLException {
		this.dataSource = dataSource;
		this.databaseMetaData = dataSource.getConnection().getMetaData();
		this.schemaName = schemaName;
		this.tableName = tableName;
	}

	@Override
	public void setTableColumns() {

		try (ResultSet columnsResultSet = this.databaseMetaData.getColumns(null, this.schemaName, this.tableName, WILDCARD)) {
			if (this.columns == null) {
				this.columns = new LinkedHashSet<>();
			}

			while (columnsResultSet.next()) {
				Column col = new Column(this);
				String colName = columnsResultSet.getString(COLUMN_NAME);
				col.setColumnName(colName);
				DataType dataType = getMySqlDataType(columnsResultSet.getInt(DATA_TYPE));
				col.setType(dataType);
				col.setAutoincrement(columnsResultSet.getString(IS_AUTOINCREMENT).equals(YES));
				col.setNullable(columnsResultSet.getString(IS_NULLABLE).equals(YES));
				this.columns.add(col);
				String columnDefault = columnsResultSet.getString("COLUMN_DEF");
				col.setDefaultValue(columnDefault == null ? "NULL" : col.isAnyDateTimeAndCurrentTimestamp(columnDefault) ? columnDefault : "'" + columnDefault + "'");
				col.setDefaultable(columnDefault != null);

				log.debug("Added column {} to table {}", colName, tableName);
			}
			log.info("Set columns for table {}", tableName);

		} catch (SQLException e) {
			log.error("SQLException in setTableColumns with tableName {}.\n {}", this.tableName, e);
		}
	}

	@Override
	public void setTableSize() {
		NamedParameterJdbcTemplate template = new NamedParameterJdbcTemplate(this.dataSource);
		@SuppressWarnings("ConstantConditions") int sizeInMb = template
				.queryForObject(QUERY_SIZE_ONE_TABLE, new MapSqlParameterSource().addValue("tableSchema", this.schemaName)
				.addValue("tableName", this.tableName), (rs, rowNum) -> rs.getInt("size-MB"));
		log.debug("Size for table {} is {} MB", this.tableName, sizeInMb);
		this.setTableSizeInMb(BigDecimal.valueOf(sizeInMb));
	}

	/**
	 * sets columns size, decimal precision, signed/unsigned, Extra column
	 * because this sort of data is missing in the databaseMetaData object or is inconsistent
	 */
	@Override
	public void setTableDetailsAndExtras() {
		JdbcTemplate template = new JdbcTemplate(this.dataSource);
		log.debug("Running DESCRIBE for type details on table {}", this.getTableName());
		List<Map<String, Object>> rows = template.queryForList("DESCRIBE " + this.getTableName());
		Map<String, Column> columnsAsMap = this.getColumnsAsMap();
		rows.forEach(rowMap -> columnsAsMap.get(rowMap.get("Field")).updateColumnFromTypeString(rowMap.get("Type")));
		rows.forEach(rowMap -> columnsAsMap.get(rowMap.get("Field")).updateColumnFromExtraString(rowMap.get("Extra")));
		log.info("Set type details for table {}", tableName);
	}

	@Override
	public void setTableRowLengthApprox() {
		NamedParameterJdbcTemplate template = new NamedParameterJdbcTemplate(this.dataSource);
		log.debug("Checking row length approx on table {}", this.tableName);
		@SuppressWarnings("ConstantConditions") long approxTableRowCount = template
				.queryForObject("SHOW TABLE STATUS WHERE name = :name;", new MapSqlParameterSource().addValue("name", this.tableName), (rs, rowNum) -> rs.getLong("Rows"));
		log.info("Approx number of rows for table {} is {} rows", this.tableName, approxTableRowCount);
		this.setNumberOfRowsApprox(approxTableRowCount);
	}


	@Override
	public void setTableIndices() {
		try (ResultSet indicesRs = this.databaseMetaData.getIndexInfo(null, this.schemaName, this.tableName, false, false)) {
			Map<String, Index> indices = new HashMap<>();
			while (indicesRs.next()) {
				String indexTableReference = indicesRs.getString("TABLE_NAME");
				String indexName = indicesRs.getString("INDEX_NAME");
				short indexOrdinalPosition = indicesRs.getShort("ORDINAL_POSITION");
				String indexColumnName = indicesRs.getString("COLUMN_NAME");
				String ascDesc = indicesRs.getString("ASC_OR_DESC");
				log.debug("Dealing with new index {} for table {}", indexName, this.tableName);
				if (!"PRIMARY".equals(indexName)) { // we dont want to re-index the primary key
					if (indexOrdinalPosition > 1 && indices.containsKey(indexName)) {
						Index index = indices.get(indexName);
						index.getColumnReferences().add(indexColumnName);
					} else {
						Index index = new Index(indexTableReference, indexName, new LinkedHashSet<>(Collections.singleton(indexColumnName)), "A".equals(ascDesc));
						indices.put(indexName, index);
					}
				}
			}
			this.indices = new LinkedHashSet<>(indices.values());
			log.info("Set {} indices for table {}", indices.size(), this.tableName);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void setTablePrimaryKeys() {
		try (ResultSet primaryKeyRs = this.databaseMetaData.getPrimaryKeys(null, this.schemaName,this.getTableName())) {
			Set<Key> primaryKeys = new HashSet<>();
			while (primaryKeyRs.next()) {
				String keyName = primaryKeyRs.getString("COLUMN_NAME");
				String keyType = primaryKeyRs.getString("PK_NAME");
				Key key;
				if (keyType.equals("PRIMARY")) {
					key = new Key(true, this.tableName, keyName);
					primaryKeys.add(key);
				}
				log.debug("column name {} & pk name {}", keyName, keyType);
			}
			this.primaryKeys = primaryKeys;
			log.info("Set primary key(s) for table {}", this.tableName);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void setTableForeignKeys() {
		try (ResultSet foreignKeyRs = this.databaseMetaData.getImportedKeys(null, this.schemaName,this.tableName)) {
			Key key;
			this.foreignKeys = new HashSet<>();
			while (foreignKeyRs.next()) {
				String referencingTableName = foreignKeyRs.getString("PKTABLE_NAME");
				String referencingColumnName = foreignKeyRs.getString("PKCOLUMN_NAME");
				String fkName = foreignKeyRs.getString("FK_NAME");
				String fkColumnName = foreignKeyRs.getString("FKCOLUMN_NAME");
				key = new Key(false, referencingTableName, referencingColumnName, fkName, fkColumnName);
				this.foreignKeys.add(key);
				log.debug("Constraint {} foreign key {} referencing table {} with pk {} added", fkName, fkColumnName, referencingTableName, referencingColumnName);
			}
			log.info("Foreign key constraints set for table {}", this.tableName);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

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

	private DataType getMySqlDataType(int type) {
		switch (type) {
		case 3:
			return DataType.DECIMAL;
		case 4:
			return DataType.INT;
		case 5:
			return DataType.SMALLINT;
		case -7:
			return DataType.BIT;
		case -6:
			return DataType.TINYINT;
		case -5:
			return DataType.BIGINT;
		case 6:
			return DataType.FLOAT;
		case 8:
			return DataType.DOUBLE;
		case 1:
			return DataType.CHAR;
		case 12:
			return DataType.VARCHAR;
		case 91:
			return DataType.DATE;
		case 92:
			return DataType.TIME;
		case 93:
			return DataType.TIMESTAMP;
		case 16:
			return DataType.BOOLEAN;
		case 2004:
			return DataType.BLOB;
		}
		return null;
	}
}