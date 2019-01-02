package zhgio.myss.runners;

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

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import lombok.extern.slf4j.Slf4j;
import zhgio.myss.commons.DataType;
import zhgio.myss.contracts.Executor;
import zhgio.myss.contracts.MetaDataExplorer;
import zhgio.myss.elements.Column;
import zhgio.myss.elements.Index;
import zhgio.myss.elements.Key;
import zhgio.myss.elements.Table;

import static zhgio.myss.commons.Keyword.COLUMN_NAME;
import static zhgio.myss.commons.Keyword.DATA_TYPE;
import static zhgio.myss.commons.Keyword.IS_AUTOINCREMENT;
import static zhgio.myss.commons.Keyword.IS_NULLABLE;
import static zhgio.myss.commons.Keyword.TABLE_NAME;
import static zhgio.myss.commons.Keyword.WILDCARD;
import static zhgio.myss.commons.Keyword.YES;
import static zhgio.myss.commons.Query.QUERY_SIZE_ONE_TABLE;

/**
 * Schemes through the schemas and does stuff.
 * TODO: describe this shit better
 */
@Slf4j
public class Schemer implements Executor, MetaDataExplorer {

	private DatabaseMetaData databaseMetaData;
	private JdbcTemplate jdbcTemplate;
	private NamedParameterJdbcTemplate namedParamTemplate;

	private Schemer() {
	}

	public Schemer(DataSource dataSource) throws SQLException {
		this.databaseMetaData = dataSource.getConnection().getMetaData();
		this.jdbcTemplate = new JdbcTemplate(dataSource);
		this.namedParamTemplate = new NamedParameterJdbcTemplate(dataSource);
	}

	@Override
	public List<Table> getAllTablesFromSchema(String schemaName) {
		List<Table> tables = new ArrayList<>();
		// TABLE param filters only tables, otherwise we would get tables, views, etc
		try (ResultSet originTablesRs = this.databaseMetaData.getTables(null, schemaName, WILDCARD, new String[] { "TABLE" })) {
			log.info("Getting tables for schema pattern {}", schemaName);
			while (originTablesRs.next()) {
				String tableName = originTablesRs.getString(TABLE_NAME); // get the table name only
				//				if (tableName.equals("acquisitions")) {
				Table table = new Table(schemaName, tableName);
				tables.add(table);
				//				}
				log.debug("Fetched table {} from origin", tableName);
			}
			log.info("Got {} tables from origin", tables.size());
		} catch (SQLException e) {
			log.error("Error in connection: {}", e);
		}
		return tables;
	}

	/**
	 * Executes a native sql statement.
	 * @param table containing the sql statement.
	 */
	@Override
	public void executeStatement(Table table) {
		log.info("Executing SQL statement for table {}", table.getTableName());
		this.jdbcTemplate.execute(table.getSqlStatement());
	}

	/**
	 * Runs a native SQL query to get the number of rows in the queried table.
	 */
	@Override
	public long getTableRowLengthApprox(Table table) {
		log.debug("Checking row length approx on table {}", table.getTableName());
		@SuppressWarnings("ConstantConditions") long approxTableRowCount = namedParamTemplate
				.queryForObject("SHOW TABLE STATUS WHERE name = :name;", new MapSqlParameterSource().addValue("name", table.getTableName()), (rs, rowNum) -> rs.getLong("Rows"));
		log.info("Approx number of rows for table {} is {} rows", table.getTableName(), approxTableRowCount);
		return approxTableRowCount;
	}

	/**
	 * Gets a set of Indices out of the metaData#getIndexInfo result set.
	 */
	@Override
	public Set<Index> getTableIndicesFromMetadata(Table table) throws SQLException {
		try (ResultSet indicesRs = this.databaseMetaData.getIndexInfo(null, table.getSchemaName(), table.getTableName(), false, false)) {
			Map<String, Index> indices = new HashMap<>();
			while (indicesRs.next()) {
				String indexTableReference = indicesRs.getString("TABLE_NAME");
				String indexName = indicesRs.getString("INDEX_NAME");
				short indexOrdinalPosition = indicesRs.getShort("ORDINAL_POSITION");
				String indexColumnName = indicesRs.getString("COLUMN_NAME");
				String ascDesc = indicesRs.getString("ASC_OR_DESC");
				log.debug("Dealing with new index {} for table {}", indexName, table.getTableName());
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
			log.info("Count of {} indices for table {}", indices.size(), table.getTableName());
			return new LinkedHashSet<>(indices.values());
		}
	}

	/**
	 * Creates a list of primary keys for the specified table out of the metaData#getPrimaryKeys result set.
	 */
	@Override
	public Set<Key> getTablePrimaryKeysFromMetaData(Table table) throws SQLException {
		try (ResultSet primaryKeyRs = this.databaseMetaData.getPrimaryKeys(null, table.getSchemaName(), table.getTableName())) {
			Set<Key> primaryKeys = new HashSet<>();
			while (primaryKeyRs.next()) {
				String keyName = primaryKeyRs.getString("COLUMN_NAME");
				String keyType = primaryKeyRs.getString("PK_NAME");
				Key key;
				if (keyType.equals("PRIMARY")) {
					key = new Key(true, table.getTableName(), keyName);
					primaryKeys.add(key);
				}
				log.debug("column name {} & pk name {}", keyName, keyType);
			}
			log.info("Found primary key(s) for table {}", table.getTableName());
			return primaryKeys;
		}
	}

	/**
	 * Creates a list of foreign key constraints for the specified table out of the metaData#getImportedKeys result set.
	 */
	@Override
	public Set<Key> getTableForeignKeysFromMetaData(Table table) {
		try (ResultSet foreignKeyRs = this.databaseMetaData.getImportedKeys(null, table.getSchemaName(), table.getTableName())) {
			Key key;
			Set<Key> foreignKeys = new HashSet<>();
			while (foreignKeyRs.next()) {
				String referencingTableName = foreignKeyRs.getString("PKTABLE_NAME");
				String referencingColumnName = foreignKeyRs.getString("PKCOLUMN_NAME");
				String fkName = foreignKeyRs.getString("FK_NAME");
				String fkColumnName = foreignKeyRs.getString("FKCOLUMN_NAME");
				key = new Key(false, referencingTableName, referencingColumnName, fkName, fkColumnName);
				foreignKeys.add(key);
				log.debug("Constraint {} foreign key {} referencing table {} with pk {} added", fkName, fkColumnName, referencingTableName, referencingColumnName);
			}
			log.info("Foreign key constraints set for table {}", table.getTableName());
			return foreignKeys;
		} catch (SQLException e) {
			log.error("Error with get foreign keys: {}", e);
		}
		return null;
	}

	@Override
	public List<Map<String, Object>> getTableDetailsAndExtras(Table table) {
		log.debug("Running DESCRIBE for type details on table {}", table.getTableName());
		return jdbcTemplate.queryForList("DESCRIBE " + table.getTableName());
	}

	/**
	 * Builds a native SQL query that fetches raw table size in MB and sets it to the table field.
	 */
	@Override
	public BigDecimal getTableSize(Table table) {
		@SuppressWarnings("ConstantConditions") int sizeInMb = namedParamTemplate
				.queryForObject(QUERY_SIZE_ONE_TABLE, new MapSqlParameterSource().addValue("tableSchema", table.getSchemaName()).addValue("tableName", table.getTableName()),
						(rs, rowNum) -> rs.getInt("size-MB"));
		log.debug("Size for table {} is {} MB", table.getTableName(), sizeInMb);
		return BigDecimal.valueOf(sizeInMb);
	}

	/**
	 * Creates a base table columns out of source metadata#getColumns result set.
	 */
	@Override
	public Set<Column> getTableColumnsFromMetaData(Table table) throws SQLException {

		Set<Column> columns = new LinkedHashSet<>();
		try (ResultSet columnsResultSet = this.databaseMetaData.getColumns(null, table.getSchemaName(), table.getTableName(), WILDCARD)) {
			while (columnsResultSet.next()) {
				Column col = new Column(table);
				String colName = columnsResultSet.getString(COLUMN_NAME);
				col.setColumnName(colName);
				DataType dataType = getMySqlDataTypeFromMetaDataType(columnsResultSet.getInt(DATA_TYPE));
				col.setType(dataType);
				col.setAutoincrement(columnsResultSet.getString(IS_AUTOINCREMENT).equals(YES));
				col.setNullable(columnsResultSet.getString(IS_NULLABLE).equals(YES));
				String columnDefault = columnsResultSet.getString("COLUMN_DEF");
				col.setDefaultValue(columnDefault == null ? "NULL" : col.isAnyDateTimeAndCurrentTimestamp(columnDefault) ? columnDefault : "'" + columnDefault + "'");
				col.setDefaultable(columnDefault != null);

				columns.add(col);
				log.debug("Added column {} to table {}", colName, table.getTableName());
			}
			log.info("Set columns for table {}", table.getTableName());
		}
		return columns;

	}

	/**
	 * Helper method that takes in the database metadata type and converts it to
	 * our custom datatype (regular mysql types extended with java.sql.types)
	 */
	private DataType getMySqlDataTypeFromMetaDataType(int type) {
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
