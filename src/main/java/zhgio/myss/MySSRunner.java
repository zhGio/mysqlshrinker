package zhgio.myss;

import java.math.BigDecimal;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;

import static zhgio.myss.MySSRunner.DataType.DATETIME;
import static zhgio.myss.MySSRunner.DataType.DECIMAL;
import static zhgio.myss.MySSRunner.DataType.INT;

@Configuration
@ConditionalOnClass(DataSource.class)
@Log4j2
public class MySSRunner implements CommandLineRunner {

	// queries
	protected static final String QUERY_SIZES_ALL_TABLES = "SELECT table_name AS `Table`, round(((data_length + index_length) / 1024 / 1024), 2) `Size in MB` FROM information_schema.TABLES WHERE table_schema = ? ORDER BY 2 DESC;";
	private static final String QUERY_SIZE_ONE_TABLE = "SELECT round(((data_length + index_length) / 1024 / 1024), 2) AS `size-MB` FROM information_schema.TABLES WHERE table_schema = :tableSchema AND table_name = :tableName;";
	private static final String QUERY_TABLE_STATUS = "SHOW TABLE STATUS LIKE ':tableName';";
	private static final String QUERY_TABLE_EXACT_COUNT = "SELECT COUNT(*) FROM ?;";

	// mysql keywords
	private static final String TABLE_NAME = "TABLE_NAME";
	private static final String COLUMN_NAME = "COLUMN_NAME";
	private static final String DATA_TYPE = "DATA_TYPE";
	private static final String COLUMN_SIZE = "COLUMN_SIZE";
	private static final String IS_AUTOINCREMENT = "IS_AUTOINCREMENT";
	private static final String WILDCARD = "%";
	private static final String IS_NULLABLE = "IS_NULLABLE";
	private static final String SQL_DATA_TYPE = "SQL_DATA_TYPE";

	private static final String YES = "YES";
	private static final String DESTINATION_SCHEMA_URL = "jdbc:mysql://localhost:3306/";
	private static final String DESTINATION_SCHEMA_NAME = "ina_trimmed";
	private static final String ORIGIN_SCHEMA_URL = "jdbc:mysql://localhost:3355/";
	private static final String ORIGIN_SCHEMA_NAME = "ina";
	private static final String BACKTICK = "`";
	private static final String SPACE = " ";
	private static final String COMMA = ",";
	private static final String EMPTY_STR = "";
	public static final String DECIMAL_DIGITS = "DECIMAL_DIGITS";

	@Override
	public void run(String... args) throws Exception {

		log.info("MySQLShrinker application runner starting!");
		DatabaseMetaData originMetaData = getDataSourceOrigin().getConnection().getMetaData();

		JdbcTemplate template = new JdbcTemplate(getDataSourceOrigin());
		List<String> schemaNames = template.query("SHOW DATABASES;", (rs, rowNum) -> rs.getString(1));
		//		schemaNames.forEach(schema -> log.info(schema));

		List<Table> tables = new ArrayList<>();
		String schemaPattern = "ina";
		log.info("Getting tables for schema pattern {}", schemaPattern);

		int count = 0;

		try (ResultSet originTablesRs = originMetaData.getTables(null, schemaPattern, WILDCARD, null)) {
			while (originTablesRs.next()) {
				if (count == 10) {
					break;
				}
				String tableName = originTablesRs.getString(TABLE_NAME); // get the table name only
				//								if (tableName.equals("billing_commission_levels"))
				tables.add(new Table(schemaPattern, tableName));
				log.info("Created table {}", tableName);
				count++;
			}
		}

		tables.forEach(table -> setTableColumns(originMetaData, table));
		tables.forEach(table -> setTableSize(getDataSourceOrigin(), table));
		tables.forEach(table -> setTableRowLengthApprox(getDataSourceOrigin(), table));
		tables.forEach(table -> setTablePrimaryKey(originMetaData, table));
		tables.forEach(table -> setTableForeignKeys(originMetaData, table));
		tables.forEach(table -> setIndices(originMetaData, table));
		tables.forEach(table -> table.writeCreateStatement());

		// sort so we write down the ones that have no foreign keys first!
		tables.sort((o1, o2) -> {
			if (o1.getForeignKeys().isEmpty()) {
				return -1;
			} else if (o2.getForeignKeys().isEmpty()) {
				return 1;
			} else {
				if (o1.getForeignKeys().size() == o2.getForeignKeys().size()) {
					return 0;
				} else if (o1.getForeignKeys().size() < o2.getForeignKeys().size()) {
					return -1;
				} else {
					return 1;
				}
			}
		});

		tables.forEach(table -> table.copyTableToSchema(getDataSourceDestination()));
	}

	private void setIndices(DatabaseMetaData metaData, Table table) {
		try (ResultSet indicesRs = metaData.getIndexInfo(null, table.getSchemaName(), table.getTableName(), false, false)) {
			Map<String, Index> indices = new HashMap<>();
			while (indicesRs.next()) {

				String indexTableReference = indicesRs.getString("TABLE_NAME");
				String indexQualifier = indicesRs.getString("INDEX_QUALIFIER");
				String indexName = indicesRs.getString("INDEX_NAME");
				short indexOrdinalPosition = indicesRs.getShort("ORDINAL_POSITION");
				String indexColumnName = indicesRs.getString("COLUMN_NAME");
				String ascDesc = indicesRs.getString("ASC_OR_DESC");

				if (!"PRIMARY".equals(indexName)) { // we dont want to re-index the primary key
					if (indexOrdinalPosition > 1 && indices.containsKey(indexName)) {
						Index index = indices.get(indexName);
						index.getColumnReferences().add(indexColumnName);
					} else {
						Index index = new Index(indexTableReference, indexName, "INDEX", new LinkedHashSet<>(Collections.singleton(indexColumnName)), "A".equals(ascDesc));
						indices.put(indexName, index);
					}
				}
			}
			table.setIndices(new LinkedHashSet<>(indices.values()));
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void setTableForeignKeys(DatabaseMetaData metaData, Table table) {
		try (ResultSet foreignKeyRs = metaData.getImportedKeys(null, table.getSchemaName(), table.getTableName())) {
			Key key;
			table.setForeignKeys(new HashSet<>());
			while (foreignKeyRs.next()) {
				String referencingTableName = foreignKeyRs.getString("PKTABLE_NAME");
				String referencingColumnName = foreignKeyRs.getString("PKCOLUMN_NAME");
				String fkName = foreignKeyRs.getString("FK_NAME");
				String fkColumnName = foreignKeyRs.getString("FKCOLUMN_NAME");
				key = new Key(false, referencingTableName, referencingColumnName, fkName, fkColumnName);
				table.getForeignKeys().add(key);
				log.info("constraint {} foreign key {} referencing table {} with pk {}", fkName, fkColumnName, referencingTableName, referencingColumnName);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void setTablePrimaryKey(DatabaseMetaData metaData, Table table) {
		try (ResultSet primaryKeyRs = metaData.getPrimaryKeys(null, table.getSchemaName(), table.getTableName())) {
			while (primaryKeyRs.next()) {
				String keyName = primaryKeyRs.getString("COLUMN_NAME");
				String keyType = primaryKeyRs.getString("PK_NAME");
				Key key;
				if (keyType.equals("PRIMARY")) {
					key = new Key(true, table.getTableName(), keyName);
					table.setPrimaryKey(key);
				}
				log.info("col name {} & pk name {}", keyName, keyType);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void setTableRowLengthApprox(DataSource dataSource, Table table) {
		NamedParameterJdbcTemplate template = new NamedParameterJdbcTemplate(dataSource);
		log.info("checking row length approx on table {}", table.getTableName());
		@SuppressWarnings("ConstantConditions") long approxTableRowCount = template
				.queryForObject("SHOW TABLE STATUS WHERE name = :name;", new MapSqlParameterSource().addValue("name", table.getTableName()), (rs, rowNum) -> rs.getLong("Rows"));
		log.info("Approx number of rows for table {} is {} rows.", table.getTableName(), approxTableRowCount);
		table.setNumberOfRowsApprox(approxTableRowCount);
	}

	private void setTableSize(DataSource dataSource, Table table) {
		NamedParameterJdbcTemplate template = new NamedParameterJdbcTemplate(dataSource);
		@SuppressWarnings("ConstantConditions") int sizeInMb = template
				.queryForObject(QUERY_SIZE_ONE_TABLE, new MapSqlParameterSource().addValue("tableSchema", table.getSchemaName()).addValue("tableName", table.getTableName()),
						(rs, rowNum) -> rs.getInt("size-MB"));
		log.info("Size for table {} is {} MB.", table.getTableName(), sizeInMb);
		table.setTableSizeInMb(BigDecimal.valueOf(sizeInMb));
	}

	private void setTableColumns(DatabaseMetaData metaData, Table table) {
		String tableName = table.getTableName();
		try (ResultSet columnsResultSet = metaData.getColumns(null, metaData.getConnection().getSchema(), tableName, WILDCARD)) {
			if (table.getColumns() == null) {
				table.setColumns(new LinkedHashSet<>());
			}

			while (columnsResultSet.next()) {
				Column col = new Column(table);
				String colName = columnsResultSet.getString(COLUMN_NAME);
				col.setColumnName(colName);
				DataType dataType = getMySqlDataType(columnsResultSet.getInt(DATA_TYPE));
				col.setType(dataType);
				if (dataType != INT) { // TODO: handle other versions of int!
					col.setColumnSize(columnsResultSet.getInt(COLUMN_SIZE));
				} else {
					col.setColumnSize(calculateIntegerColumnSize(metaData, tableName, colName));
				}
				if (dataType == DECIMAL) {
					col.setDecimalDigits(columnsResultSet.getInt(DECIMAL_DIGITS));
				}
				col.setAutoincrement(columnsResultSet.getString(IS_AUTOINCREMENT).equals(YES));
				col.setNullable(columnsResultSet.getString(IS_NULLABLE).equals(YES));
				table.getColumns().add(col);
				String columnDefault = columnsResultSet.getString("COLUMN_DEF");
				col.setDefaultValue(columnDefault == null ? "NULL" : "'" + columnDefault + "'");
				col.setDefaultable(columnDefault != null);

				log.debug("Added column {} to table {}", colName, tableName);
			}
			log.info("Set columns for table {}", tableName);

		} catch (SQLException e) {
			log.error("SQLException in setTableColumns with tableName {}.\n {}", table.getTableName(), e);
		}
	}

	/**
	 * Returns the column size for the INT types since COLUMN_SIZE from the column metadata will return precision in that case and not the column size we want (i.e. INT(21)) - we want 21 as the result
	 * but the column resultSet would return 10
	 */
	private int calculateIntegerColumnSize(DatabaseMetaData metaData, String tableName, String colName) throws SQLException {
		ResultSet resultSet = metaData.getConnection().createStatement().executeQuery("SELECT " + BACKTICK + colName + BACKTICK + " FROM " + BACKTICK + tableName + BACKTICK + " WHERE 1 = 0");
		if (resultSet.next()) {
			return resultSet.getMetaData().getPrecision(1);
		} else {
			return 0;
		}
	}

	private DataType getMySqlDataType(int type) {
		switch (type) {
		case 3:
			return DataType.DECIMAL;
		case 4:
			return INT;
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
			return DataType.DATETIME;
		case 16:
			return DataType.BOOLEAN;
		}
		return null;
	}

	@Data
	@AllArgsConstructor
	@ToString(exclude = { "tableName", "type", "isAscending" })
	private class Index {
		private String tableName;
		private String indexName;
		private String type; // PRIMARY / INDEX
		private LinkedHashSet<String> columnReferences; // needs to preserve the insertion order so we can identify ordinal position of every column reference
		private boolean isAscending;
	}

	@Data
	@AllArgsConstructor
	@RequiredArgsConstructor
	@ToString(exclude = { "schemaName", "columns", "numberOfRowsExact", "numberOfRowsApprox", "jdbcTemplate" })
	@EqualsAndHashCode(exclude = "columns")
	private class Table {
		private final String schemaName;
		private final String tableName;
		private Set<Column> columns;
		private BigDecimal tableSizeInMb;
		private long numberOfRowsExact;
		private long numberOfRowsApprox; // exact can take a a while to count while approx is off by 10-20% but gives you a general idea of row count

		private JdbcTemplate jdbcTemplate;

		// table PK column name
		private Key primaryKey;

		// collection of FKs: key - referenced table name, value - foreign key column
		private Set<Key> foreignKeys;
		private Set<Index> indices;

		private String createTableStatement;

		void writeCreateStatement() {
			StringBuilder sb = new StringBuilder("CREATE TABLE ");
			sb.append(BACKTICK).append(this.getTableName()).append(BACKTICK).append(SPACE).append("(").append(SPACE);

			/*
			 * appending columns
			 */
			//@formatter:off
			this.columns.forEach(
					column ->
							sb.append(BACKTICK).append(column.getColumnName()).append(BACKTICK).append(SPACE)
							.append(column.getType()).append(appendColumnSize(column)).append(SPACE)
							.append(!column.isNullable() ? "NOT NULL " : EMPTY_STR)
							.append(column.isDefaultable() ? " DEFAULT " + column.getDefaultValue() : column.isNullable() ? " DEFAULT NULL " : EMPTY_STR)
							.append(column.isAutoincrement() ? "AUTO_INCREMENT" : EMPTY_STR)
							.append(COMMA).append(SPACE)
			);
			//@formatter:on
			if (this.primaryKey != null) {
				sb.append("PRIMARY KEY (").append(BACKTICK).append(this.primaryKey.columnName).append(BACKTICK).append(")");
			}

			/*
			 * appending indices
			 */
			if (!this.indices.isEmpty()) {
				sb.append(COMMA).append(SPACE);
				//@formatter:off
				this.indices.forEach(
					index ->
							sb.append("KEY ").append(BACKTICK).append(index.getIndexName())
							.append(BACKTICK).append(SPACE).append("(")
							.append(listToString(index.getColumnReferences()))
							.append(")").append(COMMA)
				);
				sb.deleteCharAt(sb.length() - 1); // remove last comma
			}
			//@formatter:on

			/*
			 * append constraints / foreign keys
			 */
			if (!this.foreignKeys.isEmpty()) {
				//@formatter:off
				this.foreignKeys.forEach(
					fk ->
						 sb.append(" CONSTRAINT ").append(BACKTICK)
						 .append(fk.getFkName()).append(BACKTICK).append(SPACE)
						 .append("FOREIGN KEY (").append(BACKTICK).append(fk.getFkColumnName()).append(BACKTICK).append(")")
						 .append(" REFERENCES ").append(BACKTICK).append(fk.getTableName()).append(BACKTICK)
						 .append(" (").append(BACKTICK).append(fk.getColumnName()).append(BACKTICK).append(") ").append(COMMA)
			);
				//@formatter:on
				sb.deleteCharAt(sb.length() - 1).append(")"); // remove last comma
			} else {
				sb.append(")");
			}
			this.createTableStatement = sb.toString();
		}

		public void copyTableToSchema(DataSource dataSourceDestination) {
			if (this.jdbcTemplate == null) {
				this.jdbcTemplate = new JdbcTemplate(dataSourceDestination);
			}
			log.info("copying table {} to destination");
			this.jdbcTemplate.execute(this.getCreateTableStatement());
		}
	}

	private String listToString(LinkedHashSet<String> columnReferences) {
		StringBuilder resultBuilder = new StringBuilder();
		columnReferences.forEach(colRef -> resultBuilder.append(BACKTICK).append(colRef).append(BACKTICK).append(COMMA));
		resultBuilder.deleteCharAt(resultBuilder.length() - 1);
		return resultBuilder.toString();
	}

	private String appendColumnSize(Column column) {
		if (column.getType() == DECIMAL) {
			return "(" + column.getColumnSize() + "," + column.getDecimalDigits() + ")";
		} else if (column.getType() != DATETIME) {
			return "(" + column.getColumnSize() + ")";
		} else {
			return EMPTY_STR;
		}
	}

	@Data
	@AllArgsConstructor
	@RequiredArgsConstructor
	private class Key {

		private final boolean isPrimary;
		private final String tableName;
		private final String columnName;

		private String fkName;
		private String fkColumnName;

	}

	@Data
	@AllArgsConstructor
	@RequiredArgsConstructor
	private class Column {
		@ToString.Exclude
		private final Table table;
		private String columnName;
		private DataType type;
		private int columnSize;
		@ToString.Exclude
		private boolean nullable;
		@ToString.Exclude
		private boolean autoincrement;
		@ToString.Exclude
		private boolean defaultable;
		@ToString.Exclude
		private String defaultValue;
		@ToString.Exclude
		private int decimalDigits;
	}

	enum DataType {
		INT, SMALLINT, TINYINT, BIGINT, FLOAT, DOUBLE, CHAR, VARCHAR, DATE, TIME, DATETIME, BOOLEAN, DECIMAL, BIT
	}

	@Bean(name = "dataSourceOrigin")
	public DataSource getDataSourceOrigin() {
		return DataSourceBuilder.create().url(ORIGIN_SCHEMA_URL + ORIGIN_SCHEMA_NAME).username(MySqlShrinkerApplication.USER_NAME).password(MySqlShrinkerApplication.PASSWORD)
				.driverClassName("com.mysql.jdbc.Driver").build();
	}

	@Bean(name = "dataSourceDestination")
	public DataSource getDataSourceDestination() {
		return DataSourceBuilder.create().url(DESTINATION_SCHEMA_URL + DESTINATION_SCHEMA_NAME).username("root").password("root").driverClassName("com.mysql.jdbc.Driver").build();
	}

}
