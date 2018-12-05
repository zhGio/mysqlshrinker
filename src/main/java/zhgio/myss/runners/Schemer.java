package zhgio.myss.runners;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;

import lombok.extern.slf4j.Slf4j;
import zhgio.myss.contracts.Executor;
import zhgio.myss.elements.Table;

import static zhgio.myss.commons.Keyword.TABLE_NAME;
import static zhgio.myss.commons.Keyword.WILDCARD;

/**
 * Read metadata from the dataSource and initialize the list of tables;
 */
@Slf4j
public class Schemer implements Executor {

	private DataSource dataSource;
	private JdbcTemplate jdbcTemplate;
	private DatabaseMetaData databaseMetaData;

	private Schemer() {
	}

	public Schemer(DataSource dataSource) throws SQLException {
		this.dataSource = dataSource;
		this.databaseMetaData = dataSource.getConnection().getMetaData();
		this.jdbcTemplate = new JdbcTemplate(this.dataSource);
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
				Table table = new Table(schemaName, tableName, this.dataSource);
				tables.add(table);
				//				}
				log.debug("Fetched table {} from origin", tableName);
			}
			log.info("Got {} tables from origin", tables.size());
		} catch (SQLException e) {
			log.error("Error in connection: ", e);
		}
		return tables;
	}

	@Override
	public void executeStatement(Table table) {
		log.info("Executing SQL statement for table {}", table.getTableName());
		this.jdbcTemplate.execute(table.getSqlStatement());
	}

}
